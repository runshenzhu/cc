DROP TABLE IF EXISTS tweets_q6;

CREATE TABLE tweets_q6 (
  id bigint,
  text varchar(640) character set utf8mb4
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci ENGINE=MyISAM
PARTITION BY HASH(id)
PARTITIONS 20;
