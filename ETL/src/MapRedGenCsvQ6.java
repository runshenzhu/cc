import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by jessesleep on 11/22/15.
 */
public class MapRedGenCsvQ6 {

    public static class ShardQ5Mapper
            extends Mapper<Object, Text, LongWritable, Text> {

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            LongWritable keyOut = new LongWritable();

            Q2Q3TweetStructure tweet = new Q2Q3TweetStructure(value.toString());

            keyOut.set(tweet.id);

            context.write(keyOut, value);
        }
    }

    public static class TidShardPartitioner extends Partitioner<LongWritable, Text> {

        @Override
        public int getPartition(LongWritable key, Text value, int numReduceTasks){
            long tweet_id = key.get();
            return TweetProcessor.shardByLongId(numReduceTasks, tweet_id);
        }
    }

    public static class CsvTransformQ5Reducer
            extends Reducer<LongWritable,Text, NullWritable, Text> {

        @Override
        public void reduce(LongWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            for ( Text value : values ) {
                Text valueOut = new Text();
                Q2Q3TweetStructure tweet = new Q2Q3TweetStructure(value.toString());
                valueOut.set(tweet.toEscapeStringQ5());
                context.write(NullWritable.get(), valueOut);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 3) {
            System.err.println("Usage: MapRedGenCsvQ6 <Q2Q3_prepare_in> <Q5_csv_out> <shardCount>");
            System.exit(1);
        }

        Job job = Job.getInstance(conf, "CsvQ5");

        job.setPartitionerClass(TidShardPartitioner.class);
        job.setJarByClass(MapRedGenCsvQ6.class);
        job.setMapperClass(ShardQ5Mapper.class);
        job.setReducerClass(CsvTransformQ5Reducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(Integer.valueOf(args[2]));

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
