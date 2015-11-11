DROP TABLE IF EXISTS hashtags;

CREATE TABLE hashtags (
  hashtag varchar(140) character set utf8mb4,
  create_at int,
  count int,
  user_list text character set utf8mb4,
  tweet varchar(640) character set utf8mb4
) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin ENGINE=InnoDB;
