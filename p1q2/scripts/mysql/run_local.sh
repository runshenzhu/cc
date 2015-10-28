TMP_FILE='/tmp/input.csv'
DATA_DIR='/home/ubuntu/data/'

mysql -u root -pD807isfuckingyou < init.sql
[ -f $TMP_FILE ] && rm $TMP_FILE

for i in `ls $DATA_DIR`
do
	echo 'loading to sql' $i
	cp $DATA_DIR$i $TMP_FILE
	time mysql -u root -pD807isfuckingyou --local-infile twitter < import_data.sql
done

mysql -u root -pD807isfuckingyou twitter < post.sql
