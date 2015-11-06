import java.io.IOException;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class MapRedPutHBaseQ2 {
    private Put createPut(TweetStructure tweet){
        byte [] rowKey = Bytes.add(Bytes.toBytes(tweet.userId), Bytes.toBytes(tweet.timestamp));
    }

    /**
     * Implements the <code>Mapper</code> that takes the lines from the input
     * and outputs <code>Put</code> instances.
     */
    static class ImportMapper
            extends Mapper<LongWritable, Text, ImmutableBytesWritable, Mutation> {

        /**
         * Maps the input.
         *
         * @param offset The current offset into the input file.
         * @param line The current line of the file.
         * @param context The task context.
         * @throws java.io.IOException When mapping the input fails.
         */
        @Override
        public void map(LongWritable offset, Text line, Context context)
                throws IOException {
            try {
                TweetStructure tweet = new TweetStructure(line.toString());
                byte[] md5Url = DigestUtils.md5(link);
                Put put = new Put(md5Url);

                context.write(new ImmutableBytesWritable(md5Url), put);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Main entry point.
     *
     * @param args  The command line parameters.
     * @throws Exception When running the job fails.
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();

        if( args.length != 2 ){
            System.out.println("MapRedPutHBaseQ2 <inputFile> <outputTableName>" +
                    "\nNote that the table must exist in HBase in advance");
            System.exit(1);
        }

        Job job = Job.getInstance(conf, "Import from file " + args[0] +
                " into table " + args[1]);
        job.setJarByClass(MapRedPutHBaseQ2.class);
        job.setMapperClass(ImportMapper.class);
        job.setOutputFormatClass(TableOutputFormat.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, args[1]);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Writable.class);
        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // run the job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}