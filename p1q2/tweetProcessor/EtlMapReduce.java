import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class EtlMapReduce {

  public static class TweetMapper
          extends Mapper<Object, Text, NullWritable, Text>{

    private TweetProcessor tweetProcessor;

    private Configuration conf;

    private HbaseHandler hbaseHandler;

    @Override
    public void setup(Context context) throws IOException,
            InterruptedException {
      conf = context.getConfiguration();
      URI[] dictURIs = Job.getInstance(conf).getCacheFiles();
      Path sentimentDictFile = new Path(dictURIs[0].getPath());
      Path censorDictFile = new Path(dictURIs[1].getPath());

      String hbaseUrl = conf.get("hbase.url","localhost");
      String tableName = conf.get("table.name", "tweet");
      String familyName = conf.get("family.name", "obgun");

      System.out.println("Hbase url: "+hbaseUrl);
      System.out.println("Hbase table: "+tableName);
      System.out.println("Hbase family: "+familyName);

      String sentimentDictFileName = sentimentDictFile.getName().toString();
      String censorDictFileName = censorDictFile.getName().toString();

      BufferedReader sentimentDictRd = new BufferedReader(new FileReader(sentimentDictFileName));
      BufferedReader censorDictRd = new BufferedReader(new FileReader(censorDictFileName));

      tweetProcessor = new TweetProcessor(sentimentDictRd, censorDictRd);
      hbaseHandler = new HbaseHandler("ec2-52-91-91-20.compute-1.amazonaws.com", tableName, familyName);
    }

    @Override
    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
      Text valueOut = new Text();
      String line = value.toString();

      TweetStructure tweet = tweetProcessor.handleLine(line);
      // Skip bad line
      if( tweet == null )
        return;

      // May need some encoding technique
      valueOut.set(tweet.toString());

      // Insert into Hbase
      hbaseHandler.insert(tweet);

      context.write(NullWritable.get(), valueOut);
    }
  }

  public static class IntSumReducer
          extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
    ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
    String[] remainingArgs = optionParser.getRemainingArgs();
    if (remainingArgs.length != 7) {
      System.err.println("Usage: etl <in> <out> <sentimentDictFile> " +
              "<censorDictFile> <HBaseURL> <tableName> <familyName>");
      System.exit(2);
    }
    conf.set("mapred.textoutputformat.separator", "");
    conf.set("hbase.url", remainingArgs[4]);
    conf.set("table.name", remainingArgs[5]);
    conf.set("family.name", remainingArgs[6]);

    Job job = Job.getInstance(conf, "etl");
    job.setJarByClass(EtlMapReduce.class);
    job.setMapperClass(TweetMapper.class);
//    job.setCombinerClass(IntSumReducer.class);
//    job.setReducerClass(IntSumReducer.class);
//    job.setOutputKeyClass(Text.class);
//    job.setOutputValueClass(IntWritable.class);

    // We have no reducer
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setNumReduceTasks(0);

    FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));
    // Add the dict files to cache
    job.addCacheFile(new Path(remainingArgs[2]).toUri());
    job.addCacheFile(new Path(remainingArgs[3]).toUri());

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}