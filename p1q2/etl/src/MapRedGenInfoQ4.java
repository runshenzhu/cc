import java.io.IOException;
import java.util.*;

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

public class MapRedGenInfoQ4 {

    public static class HashTagMapper
            extends Mapper<Object, Text, Text, Text>{

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            Text valueOut = new Text();
            Text keyOut = new Text();

            String line = value.toString();
            TweetStructure tweet = new TweetStructure(line);
            List<HashtagStructure> hashtags = TweetProcessor.getHashtagStruct(tweet);

            for( HashtagStructure h : hashtags ){
                keyOut.set(h.hashtag);
                valueOut.set(h.toJsonLine());
                context.write(keyOut, valueOut);
            }
        }
    }

    public static class HashTagShardPartitioner extends Partitioner<Text, Text> {

        @Override
        public int getPartition( Text key, Text value, int numReduceTasks ){
            String hashtag = key.toString();
            return TweetProcessor.shardByStr(numReduceTasks, hashtag);
        }
    }

    public static class HashTagReducer
            extends Reducer<LongWritable,Text, NullWritable, Text> {

        public void reduce(LongWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            HashMap<Integer, HashtagStructure> hashtagMap = new HashMap<Integer, HashtagStructure>();
            HashMap<Integer, HashSet<Long>> userCheck = new HashMap<Integer, HashSet<Long>>();

            // Receive all tweets that contains a hash key
            for( Text text : values ){
                // The mapper's output always has only 1 user, but may have user count more than 1
                // Depend on how we treat duplicate hashtag in the same tweet
                HashtagStructure tag = new HashtagStructure(text.toString());
                int dayTime = TweetProcessor.skewedTimestampRoundToDay(tag.skewedTimestamp);
                long userId = tag.userList.get(0);

                if(hashtagMap.containsKey(dayTime)){
                    HashtagStructure tagRecord = hashtagMap.get(dayTime);
                    // Update the exsiting record
                    // Check duplicated user in the same day
                    if( !userCheck.get(dayTime).contains(userId)){
                        tagRecord.userList.add(userId);
                        userCheck.get(dayTime).add(userId);
                    }
                    tagRecord.count += tag.count;

                    // Update the source text
                    // Primary key: time ( smaller one wins ), Secondary key: string order( the one rank higher wins )
                    if( tag.skewedTimestamp < tagRecord.skewedTimestamp
                            || ( tag.skewedTimestamp == tagRecord.skewedTimestamp &&
                            tag.sourceText.compareTo(tagRecord.sourceText) < 0 )){
                        tagRecord.sourceText = tag.sourceText;
                    }

                } else {
                    hashtagMap.put(dayTime, tag);
                    HashSet<Long> set = new HashSet<Long>();
                    set.add(userId);
                    userCheck.put(dayTime, set);
                }
            }

            // Output the map
            for(Map.Entry entry : hashtagMap.entrySet() ){
                Text valueOut = new Text();
                HashtagStructure htag = (HashtagStructure)(entry.getValue());
                // Sort the user in ascending order
                Collections.sort( htag.userList );
                valueOut.set(htag.toJsonLine());
                context.write(NullWritable.get(), valueOut);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        if (args.length != 3) {
            System.err.println("Usage: MapRedExtractInfo <in> <out> <shardCount>");
            System.exit(1);
        }

        Job job = Job.getInstance(conf, "Q4");

        job.setPartitionerClass(HashTagShardPartitioner.class);
        job.setJarByClass(MapRedExtractInfo.class);
        job.setMapperClass(HashTagMapper.class);
        job.setReducerClass(HashTagReducer.class);

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