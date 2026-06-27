CREATE DATABASE IF NOT EXISTS movie_recommend
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

USE movie_recommend;

DROP TABLE IF EXISTS rec_movies;
DROP TABLE IF EXISTS ratings;
DROP TABLE IF EXISTS movies;
DROP TABLE IF EXISTS users;

CREATE TABLE `users` (
  `user_id`   int NOT NULL AUTO_INCREMENT,
  `username`  varchar(50) NOT NULL,
  `password`  varchar(32) NOT NULL,
  `is_admin`  tinyint DEFAULT '0',
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`user_id`),
  UNIQUE KEY `username` (`username`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `movies` (
  `movie_id`   int NOT NULL AUTO_INCREMENT,
  `title`      varchar(200) NOT NULL,
  `genres`     varchar(100) DEFAULT NULL,
  `avg_rating` double DEFAULT '0',
  `year`       int DEFAULT NULL,
  `poster_url` varchar(500) DEFAULT NULL,
  PRIMARY KEY (`movie_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `ratings` (
  `id`        int NOT NULL AUTO_INCREMENT,
  `user_id`   int NOT NULL,
  `movie_id`  int NOT NULL,
  `rating`    tinyint NOT NULL,
  `timestamp` bigint DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_user_movie` (`user_id`,`movie_id`),
  KEY `movie_id` (`movie_id`),
  CONSTRAINT `ratings_ibfk_1` FOREIGN KEY (`user_id`) REFERENCES `users` (`user_id`),
  CONSTRAINT `ratings_ibfk_2` FOREIGN KEY (`movie_id`) REFERENCES `movies` (`movie_id`),
  CONSTRAINT `ratings_chk_1` CHECK (`rating` between 1 and 5)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE `rec_movies` (
  `id`       int NOT NULL AUTO_INCREMENT,
  `user_id`  int NOT NULL,
  `movie_id` int NOT NULL,
  `score`    double DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `user_id` (`user_id`),
  KEY `movie_id` (`movie_id`),
  CONSTRAINT `rec_movies_ibfk_1` FOREIGN KEY (`user_id`) REFERENCES `users` (`user_id`),
  CONSTRAINT `rec_movies_ibfk_2` FOREIGN KEY (`movie_id`) REFERENCES `movies` (`movie_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
