CREATE INDEX tweets_timestamp_index ON tweets (user_id ASC, create_at ASC) USING BTREE;
CREATE USER 'obgun' IDENTIFIED BY 'D807isfuckingyou';
GRANT ALL PRIVILEGES ON twitter.* TO 'obgun'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;
