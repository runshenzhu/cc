for i in `seq 0 99`; do
	echo "evil on $i"
	echo "insert into tweets2 (user_id, create_at, ret_str) SELECT user_id, create_at, group_concat(concat(cast(id as char), ':', cast(sentiment as char), ':', text) separator '\n') ret_str FROM tweets partition (p$i) GROUP BY user_id, create_at;" | mysql -u root -pD807isfuckingyou twitter
done
