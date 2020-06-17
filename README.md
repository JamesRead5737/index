# index

Create database with mysql root user with following command:

CREATE DATABASE `indexes` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;

Grant permissions to crawler user with following command:

GRANT ALL ON indexes.* to crawler@localhost;

