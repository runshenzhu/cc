DROP DATABASE IF EXISTS twitter;
CREATE DATABASE twitter CHARACTER SET utf8 COLLATE utf8_general_ci;
USE twitter;

DROP TABLE IF EXISTS tweets;

CREATE TABLE tweets (
  id bigint,
  user_id bigint,
  create_at timestamp,
  text varchar(640) character set utf8,
  sentiment int
)  CHARACTER SET utf8 COLLATE utf8_general_ci ENGINE = INNODB;

