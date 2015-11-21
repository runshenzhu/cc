import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

/**
 * Created by jessesleep on 11/21/15.
 */
public class MapRedPutHBaseQ2Q3 {
    private static final byte[] FAMILY = Bytes.toBytes("o");
    private static final byte[] SENTIMENTSCORE = Bytes.toBytes("s");
    private static final byte[] IMPACTSCORE = Bytes.toBytes("i");
    private static final byte[] SENSOREDTEXT = Bytes.toBytes("t");

    private static byte[] getRowkey( Q2Q3TweetStructure tweet ){
        // int hashvalue = String.valueOf(tweet.userId).hashCode();
        //byte [] rowkey = Bytes.add(Bytes.toBytes(hashvalue),
        //        Bytes.toBytes(tweet.userId), Bytes.toBytes(tweet.skewedTimestamp));

        byte [] rowkey = Bytes.toBytes(tweet.userId);
        return rowkey;
    }

    private static Put createPut(Q2Q3TweetStructure tweet){
        byte [] rowkey = getRowkey(tweet);
        Put put = new Put(rowkey, tweet.id);

        byte [] sTmp = Bytes.toBytes(tweet.skewedTimestamp);

        put.add(FAMILY, Bytes.add(sTmp, SENTIMENTSCORE), Bytes.toBytes(tweet.sentimentScore));
        put.add(FAMILY, Bytes.add(sTmp, IMPACTSCORE), Bytes.toBytes(tweet.impactScore));
        put.add(FAMILY, Bytes.add(sTmp, SENSOREDTEXT), Bytes.toBytes(tweet.censoredText));
        return put;
    }

    public static class PutHBaseQ2Q3Mapper
            extends Mapper<Object, Text, ImmutableBytesWritable, Put> {

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            Q2Q3TweetStructure tweet = new Q2Q3TweetStructure(line);
            Put put = createPut(tweet);
            context.write( new ImmutableBytesWritable(put.getRow()), put);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        if (args.length != 2 ) {
            System.err.println("Usage: MapRedPutHBaseQ2Q3 <in> <tableName>");
            System.exit(2);
        }

        Job job = new Job(conf, "MapRedLangModel");
        job.setJarByClass(MapRedPutHBaseQ2Q3.class);
        job.setMapperClass(PutHBaseQ2Q3Mapper.class);

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        job.setNumReduceTasks(0);
        TableMapReduceUtil.initTableReducerJob(args[1], null, job);
        TableMapReduceUtil.addDependencyJars(job);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
