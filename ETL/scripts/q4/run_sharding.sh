TMP_FILE='/tmp/input.csv'
DATA_DIR='/home/ubuntu/data/'

mysql -u root -pD807isfuckingyou twitter < init.sql
[ -e $TMP_FILE ] && rm $TMP_FILE

echo 'loading to sql' $1
ln -s $DATA_DIR/p$1.csv $TMP_FILE
time mysql -u root -pD807isfuckingyou --local-infile twitter < import_data.sql

mysql -u root -pD807isfuckingyou twitter < post.sql
