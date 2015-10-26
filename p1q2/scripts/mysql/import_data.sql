LOAD DATA INFILE '/tmp/input.csv'
INTO TABLE tweets
FIELDS TERMINATED BY ','
      ESCAPED BY ''
      ENCLOSED by '"'
LINES TERMINATED BY '\n'
(id, user_id, @var1, text, sentiment)
SET create_at = STR_TO_DATE(@var1, '%b-%d-%Y+%T');
