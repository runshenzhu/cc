//import java.io.IOException;
//import java.util.Iterator;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.client.Put;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
//import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
//import org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//
//public class MapRedPutHBaseQ2Q3 {
//    private static final byte[] FAMILY = Bytes.toBytes("o");
//    private static final byte[] TWEETID = Bytes.toBytes("tid");
//    private static final byte[] SENTIMENTSCORE = Bytes.toBytes("sen");
//    private static final byte[] IMPACTSCORE = Bytes.toBytes("imp");
//    private static final byte[] SENSOREDTEXT = Bytes.toBytes("tex");
//
//    public static byte[] transformIntoRowkey( Long userid, int skewedTimestamp ){
//        int hashvalue = String.valueOf(userid).hashCode();
//        byte [] uidTime = Bytes.add(Bytes.toBytes(userid), Bytes.toBytes(skewedTimestamp));
//        byte [] rowkey = Bytes.add(Bytes.toBytes(hashvalue), uidTime);
//        return rowkey;
//    }
//
//    public static Put createPut(Q2Q3TweetStructure tweet){
//        byte [] rowkey = transformIntoRowkey(tweet.userId, tweet.skewedTimestamp);
//        Put put = new Put(rowkey);
//        put.add(FAMILY, SENTIMENTSCORE, Bytes.toBytes(tweet.sentimentScore));
//        put.add(FAMILY, IMPACTSCORE, Bytes.toBytes(tweet.impactScore));
//        put.add(FAMILY, SENSOREDTEXT, Bytes.toBytes(tweet.censoredText));
//        put.add(FAMILY, TWEETID, Bytes.toBytes(tweet.id));
//        return put;
//    }
//
//    static class IndenticalMapper
//            extends Mapper<LongWritable, Text, LongWritable, Text > {
//
//
//        @Override
//        public void map(LongWritable offset, Text line, Context context)
//                throws IOException, InterruptedException {
//            context.write(offset, line);
//        }
//    }
//
//
//    static class PutReducer
//            extends Reducer<LongWritable, Text, ImmutableBytesWritable, Put>{
//
//        @Override
//        public void reduce( LongWritable key, Iterable<Text> values, Context context )
//                throws IOException, InterruptedException {
//            for( Text value : values ){
//                Q2Q3TweetStructure tweet = new Q2Q3TweetStructure(value.toString());
//                Put put = createPut(tweet);
//                context.write(new ImmutableBytesWritable(put.getRow()), put);
//            }
//        }
//    }
//    /**
//     * Main entry point.
//     *
//     * @param args  The command line parameters.
//     * @throws Exception When running the job fails.
//     */
//    public static void main(String[] args) throws Exception {
//        Configuration conf = HBaseConfiguration.create();
//
//        if( args.length != 2 ){
//            System.out.println("MapRedPutHBaseQ2Q3 <inputFile> <outputTableName>" +
//                    "\nNote that the table must exist in HBase in advance");
//            System.exit(1);
//        }
//
//        Job job = Job.getInstance(conf, "Import from file " + args[0] +
//                " into table " + args[1]);
//
//        job.setJarByClass(MapRedPutHBaseQ2Q3.class);
//        job.setMapperClass(IndenticalMapper.class);
//        job.setReducerClass(PutReducer.class);
//
//        job.setMapOutputKeyClass(LongWritable.class);
//        job.setMapOutputValueClass(Text.class);
//
//        TableMapReduceUtil.initTableReducerJob(args[2], null, job);
//        TableMapReduceUtil.addDependencyJars(job);
//        job.setNumReduceTasks(32);
//
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        // run the job
//        System.exit(job.waitForCompletion(true) ? 0 : 1);
//    }
//}