import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
public class MapRedGenCsvQ4 {

    public static class HashtagShardMapper
            extends Mapper<Object, Text, Text, Text>{

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Text keyOut = new Text();

            HashtagStructure hashtag = new HashtagStructure(value.toString());

            keyOut.set(hashtag.hashtag);

            context.write(keyOut, value);
        }
    }

    public static class HashtagShardPartitioner extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numReduceTasks){
            String hashtag = key.toString();
            return TweetProcessor.shardByStr(numReduceTasks, hashtag);
        }
    }

    public static class HashTagCsvTransformReducer
            extends Reducer<Text,Text, NullWritable, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            for ( Text value : values ) {
                Text valueOut = new Text();
                HashtagStructure hashtag = new HashtagStructure(value.toString());
                valueOut.set(hashtag.toEscapeString());
                context.write(NullWritable.get(), valueOut);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 3) {
            System.err.println("Usage: MapRedGenCsvQ4 <in> <out> <shardCount>");
            System.exit(1);
        }

        Job job = Job.getInstance(conf, "CsvQ4");

        job.setJarByClass(MapRedGenCsvQ4.class);
        job.setMapperClass(HashtagShardMapper.class);
        job.setReducerClass(HashTagCsvTransformReducer.class);
        job.setPartitionerClass(HashtagShardPartitioner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(Integer.valueOf(args[2]));

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
