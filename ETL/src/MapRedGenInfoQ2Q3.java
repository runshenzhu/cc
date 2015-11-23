import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

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

public class MapRedGenInfoQ2Q3 {

    public static class RefineMapper
            extends Mapper<Object, Text, LongWritable, Text>{

        private TweetProcessor tweetProcessor;

        private Configuration conf;

        @Override
        public void setup(Context context) throws IOException,
                InterruptedException {
            conf = context.getConfiguration();
            URI[] dictURIs = Job.getInstance(conf).getCacheFiles();
            Path sentimentDictFile = new Path(dictURIs[0].getPath());
            Path censorDictFile = new Path(dictURIs[1].getPath());

            String sentimentDictFileName = sentimentDictFile.getName().toString();
            String censorDictFileName = censorDictFile.getName().toString();

            BufferedReader sentimentDictRd = new BufferedReader(new FileReader(sentimentDictFileName));
            BufferedReader censorDictRd = new BufferedReader(new FileReader(censorDictFileName));

            tweetProcessor = new TweetProcessor(sentimentDictRd, censorDictRd);
        }

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Text valueOut = new Text();
            LongWritable keyOut = new LongWritable();

            String line = value.toString();
            TweetStructure origTweet = new TweetStructure(line);
            Q2Q3TweetStructure refinedTweet = tweetProcessor.refineTweetStructure(origTweet);

            valueOut.set(refinedTweet.toJsonLine());

            // User ID as keys
            keyOut.set(refinedTweet.userId);
            context.write(keyOut, valueOut);
        }
    }

    /**
     * Test the sharding setting, just a test for the following tasks
     */
    public static class ShardPartitioner extends Partitioner<LongWritable, Text> {

        @Override
        public int getPartition(LongWritable key, Text value, int numReduceTasks){
            long userId = key.get();
            return TweetProcessor.shardByLongId(numReduceTasks, userId);
        }
    }

    public static class SimpleReducer
            extends Reducer<LongWritable,Text, NullWritable, Text> {

        public void reduce(LongWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            for ( Text value : values ) {
                context.write(NullWritable.get(), value);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        if (args.length != 5) {
            System.err.println("Usage: etl <in> <out> <sentimentDictFile> <censorDictFile> <shardCount>");
            System.exit(2);
        }
        //conf.set("mapred.textoutputformat.separator", "");

        Job job = Job.getInstance(conf, "Refine");

        // Set the shard partitioner
        job.setPartitionerClass(ShardPartitioner.class);
        job.setJarByClass(MapRedGenInfoQ2Q3.class);
        job.setMapperClass(RefineMapper.class);
        job.setReducerClass(SimpleReducer.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        // Set the number of shards
        job.setNumReduceTasks(Integer.valueOf(args[4]));

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // Add the dict files to cache
        job.addCacheFile(new Path(args[2]).toUri());
        job.addCacheFile(new Path(args[3]).toUri());

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}