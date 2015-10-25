TMP_FILE='input.csv'
BASE_DIR='s3://jessesleep/ETL_test/output-00661_1/'

mysql -u root -pD807isfuckingyou < init.sql

for i in `aws s3 ls $BASE_DIR | awk '$4 ~ /^part/ {print $4}'`
do
	aws s3 cp $BASE_DIR$i ./$TMP_FILE
	mysql -u root -pD807isfuckingyou twitter < import_data.sql
	rm $TMP_FILE
done

mysql -u root -pD807isfuckingyou twitter < post.sql
