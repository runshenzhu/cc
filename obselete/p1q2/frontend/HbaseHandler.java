import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.TimeZone;

public class HbaseHandler {
    // Column name
    private static final byte[] TEXT = Bytes.toBytes("text");
    private static final byte[] SCORE = Bytes.toBytes("sentiment");
    // TODO: set the sheet name here !!!!!
    private static final String TABLE = "tweet";
    private static final byte[] FAMILY = Bytes.toBytes("obgun");

    private static String hbaseUrl;
    private static Client client;
    private static Configuration hbaseConfig;
    private static HConnection hConnection;

    public static boolean setHbase( String url ){
        hbaseUrl = url;

        hbaseConfig = HBaseConfiguration.create();
        hbaseConfig.set("hbase.zookeeper.quorum", hbaseUrl);
        hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181");

        try {
            HBaseAdmin.checkHBaseAvailable(hbaseConfig);
            hConnection = HConnectionManager.createConnection(hbaseConfig);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
            return false;
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    public static String getHbaseAnswer( String user_id, String time ){
        long timestamp = 0;

        try {
            DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            format.setTimeZone(TimeZone.getTimeZone("GMT"));
            timestamp = format.parse(time).getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        String row = user_id+","+timestamp;
        String res = "";


        // not hit
        Get get = new Get(Bytes.toBytes(row));
        HTableInterface table = null;
        try {
            //Create the CSVFormat object with the header mapping
            //table = new HTable(hbaseConfig, TABLE);
            table = hConnection.getTable(TABLE);
            get.setMaxVersions(100);
            Result r = table.get(get);
            List<KeyValue> texts = r.getColumn(FAMILY, TEXT);
            List<KeyValue> score = r.getColumn(FAMILY, SCORE);
            for(int i = texts.size()-1; i >= 0; i--){
                res += score.get(i).getTimestamp() + ":";
                res += Bytes.toInt(score.get(i).getValue())+":";
                res += Bytes.toString(texts.get(i).getValue())+"\n";
            }

        }catch (IOException e){
            e.printStackTrace();
            res = null;
        }
        finally {
            try {
                if(table != null){
                    table.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return res;
    }

}