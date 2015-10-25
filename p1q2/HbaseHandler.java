import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * Created by zrsh on 10/24/15.
 */
public class HbaseHandler {
    final private static String EMR = "ec2-54-173-198-62.compute-1.amazonaws.com";
    final private static String TABLE = "test";
    final private static String FAMILY = "obgun";
    private HTable table = null;
    public HbaseHandler() throws IOException{
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", this.EMR);
        config.set("hbase.zookeeper.property.clientPort","2181");
        config.set("hbase.master", this.EMR + ":60000");

        try {
            HBaseAdmin.checkHBaseAvailable(config);
            System.out.println("check success");
        }catch (MasterNotRunningException e){
            System.out.println("hbase master not run");
            System.exit(-1);
        }catch (ZooKeeperConnectionException e){
            System.out.println("zookeeper connect failed");
        }
        HBaseAdmin admin = new HBaseAdmin(config);
        String[] tables = admin.getTableNames();
        for(String table : tables){
            System.out.println(table);
        }
        this.table = new HTable(config, this.TABLE);
        System.out.println("connect success");
    }

/*
    public void test() throws IOException{
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        //     scan 'songdata', {COLUMNS => ['data:title'], FILTER => "SingleColumnValueFilter('data', 'title', = , 'regexstring:Total.*Water')"}

        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(
                Bytes.toBytes("data"),
                Bytes.toBytes("artist_name"),
                CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes("Usher Featuring Shyne_ Twista & Kanye West")
        );
        list.addFilter(filter1);


        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(
                Bytes.toBytes("data"),
                Bytes.toBytes("title"),
                CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes("regexstring:^(Apologies|Confessions).*")
        );
        // list.addFilter(filter2);
        // Scan s = new Scan();
        // s.addColumn(Bytes.toBytes("data"), Bytes.toBytes("artist_name"));
        // s.addColumn(Bytes.toBytes("data"), Bytes.toBytes("title"));
        // s.setFilter(list);
        ResultScanner scanner = table.getScanner(s);
        try {
            // Scanners return Result instances.
            // Now, for the actual iteration. One way is to use a while loop like so:
            for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
                // print out the row we found and the columns we were looking for
                for(KeyValue kv : rr.list()){
                    System.out.println(kv.getKey().toString() + " " + kv.getValue().toString());
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
*/
    public void insert(TweetStructure t){

        Put p = new Put(Bytes.toBytes(Long.toString(t.id)));
        p.add(Bytes.toBytes(this.FAMILY), Bytes.toBytes("user_id"), Bytes.toBytes(Long.toString(t.userId)));
        p.add(Bytes.toBytes(this.FAMILY), Bytes.toBytes("create_at"), Bytes.toBytes(t.timestamp));
        p.add(Bytes.toBytes(this.FAMILY), Bytes.toBytes("text "), Bytes.toBytes(t.censoredText));
        p.add(Bytes.toBytes(this.FAMILY), Bytes.toBytes("sentiment"), Bytes.toBytes(Long.toString(t.score)));
        try {
            table.put(p);
        }catch (IOException e){
            System.out.println("put error");
        }
    }

}
