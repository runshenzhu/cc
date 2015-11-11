DROP TABLE IF EXISTS hashtags;

CREATE TABLE hashtags (
  hashtag varchar(140) character set utf8mb4 COLLATE utf8mb4_bin,
  rank int,
  value mediumtext character set utf8mb4
)CHARACTER SET utf8mb4 COLLATE utf8mb4_bin ENGINE=MyISAM;
