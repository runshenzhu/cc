search_dir='/mnt/data/'
import_jar='/home/hadoop/import.jar'
ip='127.0.0.1'
table_name='tweet'
family='obgun'
for entry in `ls $search_dir`; do
    file=$search_dir$entry
    echo $file
    java -jar $import_jar $ip $table_name $family $file
done
/home/hadoop/lib/hbase.jar
emr.hbase.backup.Main --restore --backup-dir s3://obgunteamproject/hbaseback/ --backup-version 20151103T063636Z