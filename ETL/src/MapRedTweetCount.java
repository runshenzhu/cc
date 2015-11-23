import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by jessesleep on 11/23/15.
 */
public class MapRedTweetCount {
    private static final LongWritable LONGONE = new LongWritable(1);

    public static class UidMapper
            extends Mapper<Object, Text, LongWritable, LongWritable> {

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            LongWritable keyOut = new LongWritable();

            Q2Q3TweetStructure tweet = new Q2Q3TweetStructure(value.toString());

            keyOut.set(tweet.userId);

            context.write(keyOut, LONGONE );
        }
    }

    public static class LongSumReducer
            extends Reducer<LongWritable,LongWritable, LongWritable, LongWritable> {

        @Override
        public void reduce(LongWritable key, Iterable<LongWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            long count = 0;

            for ( LongWritable value : values ) {
                count += value.get();
            }
            context.write(key, new LongWritable(count));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: MapRedTweetCount <Q2Q3_prepare_in> <Q6_uid_count_out>");
            System.exit(1);
        }

        Job job = Job.getInstance(conf, "CsvQ6");

        job.setJarByClass(MapRedTweetCount.class);
        job.setMapperClass(UidMapper.class);
        job.setReducerClass(LongSumReducer.class);
        job.setCombinerClass(LongSumReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
