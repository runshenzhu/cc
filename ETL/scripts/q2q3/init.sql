DROP DATABASE IF EXISTS twitter;
CREATE DATABASE twitter CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;
USE twitter;

DROP TABLE IF EXISTS tweets;

CREATE TABLE tweets (
  id bigint,
  user_id bigint,
  create_at int,
  sentiment int,
  impact int,
  text varchar(640) character set utf8mb4
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci ENGINE = MYISAM
PARTITION BY HASH(user_id)
PARTITIONS 100;
