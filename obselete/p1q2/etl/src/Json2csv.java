import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Json2csv {

    public static class TweetMapper
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
            TweetStructure tweet = tweetProcessor.handleLine(line);
            // Skip bad line
            if( tweet == null )
                return;

            // Encode to escape "\n"
            valueOut.set(StringEscapeUtils.escapeJava(tweet.toEscapeString()));
            keyOut.set(tweet.id);

            context.write(keyOut, valueOut);
        }
    }

    public static class CatReducer
            extends Reducer<LongWritable,Text, NullWritable, Text> {

        public void reduce(LongWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            for ( Text value : values ) {
                String valueStr = value.toString();
                // Decode to restore the "\n"
                valueStr = StringEscapeUtils.unescapeJava(valueStr);
                Text valueOut = new Text();
                valueOut.set(valueStr);
                context.write(NullWritable.get(), valueOut);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if (remainingArgs.length != 5) {
            System.err.println("Usage: etl <in> <out> <sentimentDictFile> <censorDictFile> <reducerCount>");
            System.exit(2);
        }
        //conf.set("mapred.textoutputformat.separator", "");

        Job job = Job.getInstance(conf, "etl");
        job.setJarByClass(Json2csv.class);
        job.setMapperClass(TweetMapper.class);
        job.setReducerClass(CatReducer.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(Integer.valueOf(args[4]));

        FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));
        // Add the dict files to cache
        job.addCacheFile(new Path(remainingArgs[2]).toUri());
        job.addCacheFile(new Path(remainingArgs[3]).toUri());

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}