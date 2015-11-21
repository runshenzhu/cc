DROP TABLE IF EXISTS tweets2;

CREATE TABLE tweets2 (
  user_id bigint,
  create_at bigint,
  ret_str varchar(670) character set utf8mb4,
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci ENGINE = INNODB
PARTITION BY HASH(user_id)
PARTITIONS 100;
