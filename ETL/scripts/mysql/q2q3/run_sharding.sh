TMP_FILE='/tmp/input.csv'
DATA_DIR='/home/ubuntu/data/q2q3'
BASE_DIR='/home/ubuntu/gaocc/ETL/scripts/mysql/q2q3/'

mysql -u root -pD807isfuckingyou twitter < $BASE_DIR/init.sql
rm $TMP_FILE

echo 'loading to sql' $1
ln -s $DATA_DIR/part-r-0000$1 $TMP_FILE
time mysql -u root -pD807isfuckingyou --local-infile twitter < $BASE_DIR/import_data.sql

time mysql -u root -pD807isfuckingyou twitter < $BASE_DIR/post.sql
