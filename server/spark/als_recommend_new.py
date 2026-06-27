#!/usr/bin/env python3

import sys as _sys
import types as _types
import importlib.machinery as _machinery

if 'imp' not in _sys.modules:
    _fake_imp = _types.ModuleType('imp')
    _fake_imp.load_source = lambda name, path: _machinery.SourceFileLoader(name, path).load_module()
    _sys.modules['imp'] = _fake_imp

import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, FloatType
import pymysql


def parse_args():
    parser = argparse.ArgumentParser(description='FilmRadar ALS via HDFS')
    parser.add_argument('--user-id', type=int, required=True)
    parser.add_argument('--db-host', type=str, default='localhost')
    parser.add_argument('--db-port', type=int, default=3306)
    parser.add_argument('--db-user', type=str, default='movieapp')
    parser.add_argument('--db-password', type=str, default='movieapp123')
    parser.add_argument('--db-name', type=str, default='movie_recommend')
    parser.add_argument('--hdfs-ratings', type=str, default='/user/fsl/movie_recommend/ratings.txt')
    return parser.parse_args()


def build_jdbc_url(args):
    return ('jdbc:mysql://{}:{}/{}'
            '?useSSL=false&allowPublicKeyRetrieval=true'
            '&serverTimezone=UTC&characterEncoding=utf8').format(
                args.db_host, args.db_port, args.db_name)


def build_jdbc_props(args):
    return {
        'user': args.db_user,
        'password': args.db_password,
        'driver': 'com.mysql.cj.jdbc.Driver'
    }


def read_ratings_from_hdfs(spark, hdfs_path):
    raw_df = spark.read.option('header', 'true').option('sep', '\t').csv(hdfs_path)
    rating_df = raw_df.select(
        col('user_id').cast(IntegerType()).alias('userId'),
        col('movie_id').cast(IntegerType()).alias('movieId'),
        col('rating').cast(FloatType()).alias('rating')
    )
    return rating_df


def read_ratings_from_mysql(spark, args):
    url = build_jdbc_url(args)
    props = build_jdbc_props(args)
    df = spark.read.jdbc(url=url, table='ratings', properties=props)
    return df.select(
        col('user_id').cast('int').alias('userId'),
        col('movie_id').cast('int').alias('movieId'),
        col('rating').cast('float').alias('rating')
    )


def read_ratings(spark, args):
    hdfs_path = args.hdfs_ratings
    try:
        print('[Spark] Reading from HDFS: {}'.format(hdfs_path))
        df = read_ratings_from_hdfs(spark, hdfs_path)
        cnt = df.count()
        if cnt >= 10:
            print('[Spark] Read {} ratings from HDFS'.format(cnt))
            return df
        print('[Spark] HDFS data too few ({})'.format(cnt))
    except Exception as e:
        print('[Spark] HDFS read failed: {}'.format(e))
    print('[Spark] Falling back to MySQL...')
    df = read_ratings_from_mysql(spark, args)
    print('[Spark] Read {} ratings from MySQL'.format(df.count()))
    return df


def read_movies_from_mysql(spark, args):
    url = build_jdbc_url(args)
    props = build_jdbc_props(args)
    df = spark.read.jdbc(url=url, table='movies', properties=props)
    return df.select(col('movie_id').cast('int').alias('movieId')).distinct()


def write_results(user_id, recommendations, args):
    conn = pymysql.connect(
        host=args.db_host, port=args.db_port,
        user=args.db_user, password=args.db_password,
        database=args.db_name, charset='utf8mb4'
    )
    try:
        cursor = conn.cursor()
        cursor.execute('DELETE FROM rec_movies WHERE user_id = %s', (user_id,))
        for rec in recommendations:
            score = round(float(rec['prediction']), 4)
            cursor.execute(
                'INSERT INTO rec_movies (user_id, movie_id, score) VALUES (%s, %s, %s)',
                (user_id, rec['movieId'], score)
            )
        conn.commit()
        print('[Spark] Wrote {} recommendations for user {}'.format(len(recommendations), user_id))
    finally:
        cursor.close()
        conn.close()


def main():
    args = parse_args()

    spark = SparkSession.builder \
        .appName('FilmRadar_ALS_User_{}'.format(args.user_id)) \
        .getOrCreate()

    try:
        print('[Spark] Reading ratings from HDFS...')
        rating_df = read_ratings(spark, args)
        count = rating_df.count()
        print('[Spark] Total ratings: {}'.format(count))

        if count < 10:
            print('[Spark] ERROR: need at least 10 ratings', file=sys.stderr)
            sys.exit(1)

        print('[Spark] Reading movies from MySQL...')
        all_movies_df = read_movies_from_mysql(spark, args)

        (training, test) = rating_df.randomSplit([0.8, 0.2], seed=42)

        als = ALS(
            maxIter=15, regParam=0.05,
            userCol='userId', itemCol='movieId', ratingCol='rating',
            coldStartStrategy='drop', nonnegative=True, implicitPrefs=False
        )

        print('[Spark] Training ALS...')
        model = als.fit(training)

        predictions = model.transform(test)
        evaluator = RegressionEvaluator(
            metricName='rmse', labelCol='rating', predictionCol='prediction'
        )
        rmse = evaluator.evaluate(predictions)
        print('[Spark] RMSE = {:.4f}'.format(rmse))

        user_rated_df = rating_df.filter(col('userId') == args.user_id) \
            .select('movieId').distinct()
        print('[Spark] User rated {} movies'.format(user_rated_df.count()))

        unrated_df = all_movies_df.join(
            user_rated_df, on='movieId', how='left_anti'
        )

        if unrated_df.count() == 0:
            print('[Spark] No unrated movies', file=sys.stderr)
            sys.exit(0)

        user_df = spark.createDataFrame([(args.user_id,)], ['userId'])
        user_unrated = user_df.crossJoin(unrated_df)
        user_preds = model.transform(user_unrated)

        top_rows = user_preds \
            .filter(col('prediction').isNotNull()) \
            .orderBy(col('prediction').desc()) \
            .limit(20) \
            .select('movieId', 'prediction') \
            .collect()

        if len(top_rows) == 0:
            print('[Spark] Fallback: all movies', file=sys.stderr)
            user_all = user_df.crossJoin(all_movies_df)
            all_preds = model.transform(user_all)
            top_rows = all_preds \
                .filter(col('prediction').isNotNull()) \
                .orderBy(col('prediction').desc()) \
                .limit(20) \
                .select('movieId', 'prediction') \
                .collect()

        recs = [
            {'movieId': r.movieId, 'prediction': round(float(r.prediction), 3)}
            for r in top_rows
        ]

        print('[Spark] Top 5:')
        for rec in recs:
            print('  Movie {} -> {:.3f}'.format(rec['movieId'], rec['prediction']))

        write_results(args.user_id, recs, args)
        print('[Spark] Done.')

    finally:
        spark.stop()


if __name__ == '__main__':
    main()
