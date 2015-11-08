import java.io.IOException;

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

/**
 * Created by jessesleep on 11/5/15.
 */
public class MapRedGenCsvQ2Q3 {

    public static class ShardMapper
            extends Mapper<Object, Text, LongWritable, Text>{

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            LongWritable keyOut = new LongWritable();

            Q2Q3TweetStructure tweet = new Q2Q3TweetStructure(value.toString());

            keyOut.set(tweet.userId);

            context.write(keyOut, value);
        }
    }

    public static class UidShardPartitioner extends Partitioner<LongWritable, Text> {

        @Override
        public int getPartition(LongWritable key, Text value, int numReduceTasks){
            long userId = key.get();
            return TweetProcessor.shardByUserId(numReduceTasks, userId);
        }
    }

    public static class CsvTransformReducer
            extends Reducer<LongWritable,Text, NullWritable, Text> {

        @Override
        public void reduce(LongWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            for ( Text value : values ) {
                Text valueOut = new Text();
                Q2Q3TweetStructure tweet = new Q2Q3TweetStructure(value.toString());
                valueOut.set(tweet.toEscapeString());
                context.write(NullWritable.get(), valueOut);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 3) {
            System.err.println("Usage: MapRedGenCsvQ2Q3 <in> <out> <shardCount>");
            System.exit(1);
        }

        Job job = Job.getInstance(conf, "CsvQ2Q3");

        job.setPartitionerClass(UidShardPartitioner.class);
        job.setJarByClass(MapRedGenCsvQ2Q3.class);
        job.setMapperClass(ShardMapper.class);
        job.setReducerClass(CsvTransformReducer.class);

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
