TMP_FILE='/tmp/input.csv'
BASE_DIR='s3://jessesleep/ETL_test/final/'

mysql -u root -pD807isfuckingyou < init.sql
[ -f $TMP_FILE ] && rm $TMP_FILE

for i in `aws s3 ls $BASE_DIR | awk '$4 ~ /^part/ {print $4}'`
do
	echo 'downloading' $i
	timeout 60s aws s3 cp $BASE_DIR$i $TMP_FILE
	while [ $? -ne 0 ]; do
		echo 'downloading' $i
		timeout 60s aws s3 cp $BASE_DIR$i $TMP_FILE
	done
	echo 'loading to sql' $i
	mysql -u root -pD807isfuckingyou --local-infile twitter < import_data.sql
	rm $TMP_FILE
done

mysql -u root -pD807isfuckingyou twitter < post.sql
