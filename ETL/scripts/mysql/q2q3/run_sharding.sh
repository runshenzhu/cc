TMP_FILE='/tmp/input.csv'
DATA_DIR='/home/ubuntu/data/'
BASE_DIR='/home/ubuntu/scripts/q2q3/'

mysql -u root -pD807isfuckingyou < $BASE_DIR/init.sql
[ -e $TMP_FILE ] && rm $TMP_FILE

echo 'loading to sql' $1
ln -s $DATA_DIR/part-r-0000$1 $TMP_FILE
time mysql -u root -pD807isfuckingyou --local-infile twitter < $BASE_DIR/import_data.sql

mysql -u root -pD807isfuckingyou twitter < $BASE_DIR/post.sql
