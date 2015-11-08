import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by jessesleep on 11/7/15.
 */
public class HBasePutQ2Q3 {
    private static final byte[] FAMILY = Bytes.toBytes("o");
    private static final byte[] SENTIMENTSCORE = Bytes.toBytes("s");
    private static final byte[] IMPACTSCORE = Bytes.toBytes("i");
    private static final byte[] SENSOREDTEXT = Bytes.toBytes("t");

    private static final int LINECOUNT = 100000;

    private static byte[] getRowkey( Q2Q3TweetStructure tweet ){
        int hashvalue = String.valueOf(tweet.userId).hashCode();
        byte [] rowkey = Bytes.add(Bytes.toBytes(hashvalue),
                Bytes.toBytes(tweet.userId), Bytes.toBytes(tweet.skewedTimestamp));
        return rowkey;
    }

    private static Put createPut(Q2Q3TweetStructure tweet){
        byte [] rowkey = getRowkey(tweet);
        Put put = new Put(rowkey, tweet.id);
        put.add(FAMILY, SENTIMENTSCORE, Bytes.toBytes(tweet.sentimentScore));
        put.add(FAMILY, IMPACTSCORE, Bytes.toBytes(tweet.impactScore));
        put.add(FAMILY, SENSOREDTEXT, Bytes.toBytes(tweet.censoredText));
        return put;
    }

    public static void main(String [] args )
            throws IOException {
        if( args.length != 2 ){
            System.out.println("java HBasePutQ2Q3 <fileName> <tableName>");
            System.exit(1);
        }

        Configuration config = HBaseConfiguration.create();
        config.addResource("hbase-site.xml");
        config.set("hbase.zookeeper.quorum", "127.0.0.1");
        config.set("hbase.zookeeper.property.clientPort","2181");
        config.set("hbase.master","127.0.0.1:60010");


        try {
            HBaseAdmin.checkHBaseAvailable(config);
            System.out.println("check success");
        }catch (MasterNotRunningException e){
            System.out.println("hbase master not run " + e);
            e.printStackTrace();
            System.exit(1);
        }catch (ZooKeeperConnectionException e){
            System.out.println("zookeeper connect failed");
        }

        HTable table = new HTable(config, args[1]);

        BufferedReader fileIn = new BufferedReader( new FileReader(args[0]));
        String line;
        ArrayList<Put> putList = new ArrayList<Put>(LINECOUNT);
        int lineCount = 0;

        while( (line = fileIn.readLine()) != null ){
            lineCount += 1;
            Q2Q3TweetStructure tweet = new Q2Q3TweetStructure(line);
            putList.add(createPut(tweet));

            if( putList.size() == LINECOUNT ){
                System.out.println("File "+args[0]+" issues "+lineCount+" puts.");
                table.put(putList);
                putList.clear();
            }
        }

        if( !putList.isEmpty() ){
            System.out.println("File "+args[0]+" issues "+lineCount+" puts.");
            table.put(putList);
        }
    }
}
