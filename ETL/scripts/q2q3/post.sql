CREATE INDEX tweets_timestamp_index ON tweets (user_id ASC, create_at ASC) USING BTREE;
