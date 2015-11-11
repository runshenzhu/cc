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
import java.util.Collections;

/**
 * Created by jessesleep on 11/7/15.
 */
public class HBasePutQ4 {
    private static final byte[] FAMILY = Bytes.toBytes("o");
    private static final byte[] VALUE = Bytes.toBytes("v");

    private static final int LINECOUNT = 100000;

    private static byte[] getRowKey( HashtagStructure hashtag, int rank ){
        byte [] rowkey = Bytes.add(Bytes.toBytes(hashtag.hashtag), Bytes.toBytes(rank));
        return rowkey;
    }

    private static Put createPut( HashtagStructure hashtag, int rank ){
        byte [] rowkey = getRowKey(hashtag, rank);
        Put put = new Put(rowkey);

        StringBuilder Q4Response = new StringBuilder();

        Q4Response.append(TweetProcessor.skewedTime2Date(hashtag.skewedTimestamp)).append(":");
        Q4Response.append(String.valueOf(hashtag.count)).append(":");
        Q4Response.append(String.valueOf(hashtag.userList.get(0)));
        for( int i = 1; i < hashtag.userList.size(); ++i  ){
            Q4Response.append(",").append(hashtag.userList.get(i));
        }
        Q4Response.append(":").append(hashtag.sourceText).append("\n");

        put.add(FAMILY, VALUE, Bytes.toBytes(Q4Response.toString()));
        return put;
    }

    public static void main( String [] args ) throws IOException {
        if( args.length != 2 ){
            System.out.println("java HBasePutQ4 <fileName> <tableName>");
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
        ArrayList<Put> putList = new ArrayList<Put>();
        int lineCount = 0;

        HashtagStructure hashtagStructure = null;

        if( (line = fileIn.readLine()) != null ) {
             hashtagStructure = new HashtagStructure(line);
        }

        while( line != null ){
            ArrayList<HashtagStructure> hashtagList = new ArrayList<HashtagStructure>();

            // Handle a hashtag at a time
            do{
                lineCount += 1;

                hashtagList.add(hashtagStructure);

                if((line = fileIn.readLine()) == null ){
                    break;
                }
                // Not empty line
                hashtagStructure = new HashtagStructure(line);
            }while(hashtagStructure.hashtag.equals(hashtagList.get(0).hashtag));

            // Sort the hashtags by the Q4 order rule
            Collections.sort(hashtagList);

            // Update the put list
            for( int i = 0; i < hashtagList.size(); ++i ){
                putList.add(createPut(hashtagList.get(i), i));
            }

            // Issue if we have collected enough puts
            if( putList.size() > LINECOUNT ){
                System.out.println("File " + args[0] + " issues " + lineCount + " puts.");
                table.put(putList);
                putList.clear();
            }
        }

        if( !putList.isEmpty() ){
            System.out.println("File " + args[0] + " issues " + lineCount + " puts.");
            table.put(putList);
        }
    }
}
