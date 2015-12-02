
SET FOREIGN_KEY_CHECKS = 0;
SET UNIQUE_CHECKS = 0;

SET SESSION tx_isolation='READ-UNCOMMITTED';

SET sql_log_bin = 0;

LOAD DATA LOCAL INFILE '/tmp/input.csv'
INTO TABLE tweets_q6
FIELDS TERMINATED BY ','
      ESCAPED BY ''
      ENCLOSED by '"'
LINES TERMINATED BY '\n'
(id, text);
