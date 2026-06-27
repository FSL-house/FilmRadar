#!/usr/bin/env python3
"""
Spark ALS Collaborative Filtering - Compatible with Spark 2.4.8
Reads user ratings from MySQL, trains ALS model, writes top-5 results to MySQL.
"""

import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col
import pymysql


def parse_args():
    parser = argparse.ArgumentParser(description='Spark ALS Movie Recommendation')
    parser.add_argument('--user-id', type=int, required=True, help='Target user ID')
    parser.add_argument('--db-host', type=str, default='localhost')
    parser.add_argument('--db-port', type=int, default=3306)
    parser.add_argument('--db-user', type=str, default='movieapp')
    parser.add_argument('--db-password', type=str, default='movieapp123')
    parser.add_argument('--db-name', type=str, default='movie_recommend')
    return parser.parse_args()


def build_jdbc_url(args):
    url = (f'jdbc:mysql://{args.db_host}:{args.db_port}/{args.db_name}'
           f'?useSSL=false&allowPublicKeyRetrieval=true'
           f'&serverTimezone=UTC&characterEncoding=utf8')
    return url


def build_jdbc_props(args):
    props = {
        'user': args.db_user,
        'password': args.db_password,
        'driver': 'com.mysql.cj.jdbc.Driver'
    }
    return props


def read_ratings_from_mysql(spark, args):
    url = build_jdbc_url(args)
    props = build_jdbc_props(args)

    df = spark.read.jdbc(url=url, table='personalratings', properties=props)

    rating_df = df.select(
        col('user_id').cast('int').alias('userId'),
        col('movie_id').cast('int').alias('movieId'),
        col('rating').cast('float').alias('rating')
    )
    return rating_df


def read_all_movie_ids_from_mysql(spark, args):
    url = build_jdbc_url(args)
    props = build_jdbc_props(args)

    df = spark.read.jdbc(url=url, table='movieinfo', properties=props)

    movie_df = df.select(
        col('movie_id').cast('int').alias('movieId')
    ).distinct()
    return movie_df


def write_results_to_mysql(user_id, recommendations, args):
    conn = pymysql.connect(
        host=args.db_host,
        port=args.db_port,
        user=args.db_user,
        password=args.db_password,
        database=args.db_name,
        charset='utf8mb4'
    )

    try:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM recommendresult WHERE user_id = %s', (user_id,))
        insert_sql = (
            'INSERT INTO recommendresult (user_id, movie_id, predicted_rating) '
            'VALUES (%s, %s, %s)'
        )
        for rec in recommendations:
            cursor.execute(insert_sql, (user_id, rec['movieId'], rec['prediction']))
        conn.commit()
        print('[Spark] Wrote {} recommendations for user {}'.format(len(recommendations), user_id))
    finally:
        cursor.close()
        conn.close()


def main():
    args = parse_args()

    spark = SparkSession.builder \
        .appName('MovieALS_User_{}'.format(args.user_id)) \
        .getOrCreate()

    try:
        print('[Spark] Reading ratings from MySQL...')
        rating_df = read_ratings_from_mysql(spark, args)
        rating_count = rating_df.count()
        print('[Spark] Total ratings: {}'.format(rating_count))

        if rating_count < 10:
            print('[Spark] ERROR: Need at least 10 ratings for ALS', file=sys.stderr)
            sys.exit(1)

        print('[Spark] Reading all movie IDs...')
        all_movies_df = read_all_movie_ids_from_mysql(spark, args)

        (training, test) = rating_df.randomSplit([0.8, 0.2], seed=42)

        als = ALS(
            maxIter=10,
            regParam=0.1,
            userCol='userId',
            itemCol='movieId',
            ratingCol='rating',
            coldStartStrategy='drop',
            nonnegative=True,
            implicitPrefs=False
        )

        print('[Spark] Training ALS model...')
        model = als.fit(training)

        predictions = model.transform(test)
        evaluator = RegressionEvaluator(
            metricName='rmse',
            labelCol='rating',
            predictionCol='prediction'
        )
        rmse = evaluator.evaluate(predictions)
        print('[Spark] RMSE = {:.4f}'.format(rmse))

        user_rated_df = rating_df.filter(col('userId') == args.user_id) \
            .select('movieId').distinct()
        rated_count = user_rated_df.count()
        print('[Spark] User {} has rated {} movies'.format(args.user_id, rated_count))

        unrated_df = all_movies_df.join(
            user_rated_df, on='movieId', how='left_anti'
        )

        if unrated_df.count() == 0:
            print('[Spark] User has rated all movies, no recommendations.', file=sys.stderr)
            sys.exit(0)

        user_df = spark.createDataFrame([(args.user_id,)], ['userId'])
        user_unrated = user_df.crossJoin(unrated_df)
        user_predictions = model.transform(user_unrated)

        top_rows = user_predictions \
            .filter(col('prediction').isNotNull()) \
            .orderBy(col('prediction').desc()) \
            .limit(5) \
            .select('movieId', 'prediction') \
            .collect()

        if len(top_rows) == 0:
            print('[Spark] No valid predictions, trying all movies...', file=sys.stderr)
            user_all = user_df.crossJoin(all_movies_df)
            all_preds = model.transform(user_all)
            top_rows = all_preds \
                .filter(col('prediction').isNotNull()) \
                .orderBy(col('prediction').desc()) \
                .limit(5) \
                .select('movieId', 'prediction') \
                .collect()

        recs = [
            {'movieId': row.movieId, 'prediction': round(float(row.prediction), 3)}
            for row in top_rows
        ]

        print('[Spark] Generated {} recommendations:'.format(len(recs)))
        for rec in recs:
            print('  Movie {} -> predicted {:.3f}'.format(rec['movieId'], rec['prediction']))

        write_results_to_mysql(args.user_id, recs, args)
        print('[Spark] Recommendations written to MySQL successfully')

    finally:
        spark.stop()


if __name__ == '__main__':
    main()
