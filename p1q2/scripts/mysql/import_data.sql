LOAD DATA INFILE '/tmp/input.csv'
INTO TABLE tweets
FIELDS TERMINATED BY ','
      ESCAPED BY ''
      ENCLOSED by '"'
LINES TERMINATED BY '\n'
(id, user_id, create_at, text, sentiment);
