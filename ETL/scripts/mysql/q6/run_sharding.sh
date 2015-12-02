TMP_FILE='/tmp/input.csv'
DATA_DIR='/home/ubuntu/data/q6'
BASE_DIR='/home/ubuntu/gaocc/ETL/scripts/mysql/q6/'

mysql -u root -pD807isfuckingyou twitter < $BASE_DIR/init.sql
rm $TMP_FILE

echo 'loading to sql' $1
ln -s $DATA_DIR/p$1.csv $TMP_FILE
time mysql -u root -pD807isfuckingyou --local-infile twitter < $BASE_DIR/import_data.sql

time mysql -u root -pD807isfuckingyou twitter < $BASE_DIR/post.sql
