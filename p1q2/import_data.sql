DROP TABLE IF EXISTS tweets;
CREATE TABLE tweets (
  id bigint,
  user_id bigint,
  create_at timestamp,
  text varchar(200) character set utf8,
  sentiment int
);

LOAD DATA INFILE '/tmp/tweetout.csv'
INTO TABLE tweets
FIELDS TERMINATED BY ','
      ESCAPED BY ''
      ENCLOSED by '"'
LINES TERMINATED BY '\n'
(id, user_id, @var1, text, sentiment)
SET create_at = STR_TO_DATE(@var1, '%b-%d-%Y+%T');
