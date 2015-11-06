import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapRedExtractInfo {

    public static class TweetExtracter
            extends Mapper<Object, Text, LongWritable, Text>{

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Text valueOut = new Text();
            LongWritable keyOut = new LongWritable();

            String line = value.toString();
            TweetStructure tweet = TweetProcessor.extractTweetStructure(line);
            // Skip bad line
            if( tweet == null ) {
                System.out.println("[Info] Json parse failed: "+value.toString());
                return;
            }

            valueOut.set(tweet.toJsonLine());
            keyOut.set(tweet.id);

            context.write(keyOut, valueOut);
        }
    }

    public static class DeduplicationReducer
            extends Reducer<LongWritable,Text, NullWritable, Text> {

        public void reduce(LongWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            for ( Text value : values ) {
                context.write(NullWritable.get(), value);
                // Ignore any duplicate tweet id
                break;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: MapRedExtractInfo <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "MapRedExtractInfo");
        job.setJarByClass(MapRedExtractInfo.class);
        job.setMapperClass(TweetExtracter.class);
        job.setReducerClass(DeduplicationReducer.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}