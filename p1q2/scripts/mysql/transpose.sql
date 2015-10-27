DROP TABLE IF EXISTS tweets2;

CREATE TABLE tweets2 (
  user_id bigint,
  create_at bigint,
  ret_str varchar(670) character set utf8mb4,
  sentiment int
  -- ,PRIMARY KEY USING BTREE (id, user_id, create_at)
) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci ENGINE = INNODB
PARTITION BY HASH(user_id)
PARTITIONS 100;

--INSERT INTO tweets2
--SELECT user_id, create_at, group_concat(concat(cast(id as char), ':', cast(sentiment as char), ':', text) separator '\n') ret_str
--FROM tweets
--GROUP BY user_id, create_at;
