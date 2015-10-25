import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.rest.client.Cluster;
import org.apache.hadoop.hbase.rest.client.RemoteHTable;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;

//import java.util.List;

/**
 * Created by zrsh on 10/24/15.
 */
public class HbaseHandler {
    private String EMR;
    private String TABLE;
    private String FAMILY;
    private RemoteHTable table = null;
    public HbaseHandler(String emr, String table, String family) throws IOException{
        this.EMR = emr;
        this.TABLE = table;
        this.FAMILY = family;

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", this.EMR);
        config.set("hbase.zookeeper.property.clientPort","2181");
        // config.set("hbase.master", this.EMR + ":60000");
        Cluster cluster = new Cluster();
        cluster.add(this.EMR, 8080); // co RestExample-1-Cluster Set up a cluster list adding all known REST server hosts.

        Client client = new Client(cluster); // co RestExample-2-Client Create the client handling the HTTP communication.

        this.table = new RemoteHTable(client, this.TABLE); // co RestExample-3-Table Create a remote table instance, wrapping the REST access into a familiar interface.

        try {
            HBaseAdmin.checkHBaseAvailable(config);
            System.out.println("check success");
        }catch (MasterNotRunningException e){
            System.out.println("hbase master not run "+e);
            e.printStackTrace();
            System.exit(-1);
        }catch (ZooKeeperConnectionException e){
            System.out.println("zookeeper connect failed "+e);
            e.printStackTrace();
            System.exit(-1);
        }


        //this.table = new HTable(config, this.TABLE);
        System.out.println("connect success");
    }


    public void test() throws IOException{
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        //     scan 'songdata', {COLUMNS => ['data:title'], FILTER => "SingleColumnValueFilter('data', 'title', = , 'regexstring:Total.*Water')"}

        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(
                Bytes.toBytes(this.FAMILY),
                Bytes.toBytes("create_at"),
                CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes("2014-05-25+05:37:1")
        );
        list.addFilter(filter1);


        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(
                Bytes.toBytes("data"),
                Bytes.toBytes("title"),
                CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes("regexstring:^(Apologies|Confessions).*")
        );
        // list.addFilter(filter2);
        Scan s = new Scan();
        s.addColumn(Bytes.toBytes(this.FAMILY), Bytes.toBytes("user_id"));
        s.addColumn(Bytes.toBytes(this.FAMILY), Bytes.toBytes("create_at"));
        s.setFilter(list);
        ResultScanner scanner = table.getScanner(s);
        try {
            // Scanners return Result instances.
            // Now, for the actual iteration. One way is to use a while loop like so:
            for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
                // print out the row we found and the columns we were looking for
                for(KeyValue kv : rr.list()){
                    ByteBuffer bb = ByteBuffer.wrap(kv.getRow());
                    System.out.println(bb.getLong());
                    String qul = new String(kv.getQualifier(), "UTF-8");
                    System.out.println(qul);
                    System.out.println(new String(kv.getValue(), "UTF-8"));

                    //System.out.println(kv.getValue());
                    System.out.println("----");
                }
            }
            System.out.println("gg");

            // The other approach is to use a foreach loop. Scanners are iterable!
            // for (Result rr : scanner) {
            //   System.out.println("Found row: " + rr);
            // }
        }catch (Exception e){}
        finally {
            // Make sure you close your scanners when you are done!
            // Thats why we have it inside a try/finally clause
            scanner.close();
        }
    }

    public void insert(TweetStructure t){

        Put p = new Put(Bytes.toBytes(Long.toString(t.id)));
        p.add(Bytes.toBytes(this.FAMILY), Bytes.toBytes("user_id"), Bytes.toBytes(Long.toString(t.userId)));
        p.add(Bytes.toBytes(this.FAMILY), Bytes.toBytes("create_at"), Bytes.toBytes(t.timestamp));
        p.add(Bytes.toBytes(this.FAMILY), Bytes.toBytes("text"), Bytes.toBytes(t.censoredText));
        p.add(Bytes.toBytes(this.FAMILY), Bytes.toBytes("sentiment"), Bytes.toBytes(Long.toString(t.score)));
        try {
            table.put(p);
        }catch (IOException e){
            System.out.println("put error");
            e.printStackTrace();
        }
    }

}
