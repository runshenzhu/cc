import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
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

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


/**
 * Created by zrsh on 10/24/15.
 */
public class HbaseHandler {
    private String EMR;
    private String TABLE;
    private byte[] FAMILY;

    /****************************************************/
    final private byte[] USER_ID = Bytes.toBytes("user_id");
    final private byte[] CREATE_AT = Bytes.toBytes("create_at");
    final private byte[] TEXT = Bytes.toBytes("text");
    final private byte[] SCORE = Bytes.toBytes("sentiment");
    /****************************************************/

    private RemoteHTable table = null;
    public HbaseHandler(String emr, String table, String family) throws IOException {
        this.EMR = emr;
        this.TABLE = table;
        this.FAMILY = Bytes.toBytes(family);

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", this.EMR);
        config.set("hbase.zookeeper.property.clientPort","2181");
        // config.set("hbase.master", this.EMR + ":60000");
        Cluster cluster = new Cluster();
        cluster.add(this.EMR, 8080); // co RestExample-1-Cluster Set up a cluster list adding all known REST server hosts.

        Client client = new Client(cluster); // co RestExample-2-Client Create the client handling the HTTP communication.


        try {
            HBaseAdmin.checkHBaseAvailable(config);
            System.out.println("check success");
            this.table = new RemoteHTable(client, this.TABLE); // co RestExample-3-Table Create a remote table instance, wrapping the REST access into a familiar interface.
        }catch (MasterNotRunningException e){
            System.out.println("hbase master not run " + e);
            e.printStackTrace();
            System.exit(-1);
        }catch (ZooKeeperConnectionException e){
            System.out.println("zookeeper connect failed");
        }catch (IOException e){

        }


        //this.table = new HTable(config, this.TABLE);
        System.out.println("connect success");
    }


    public ArrayList<TweetStructure> query(String user_id, String time){
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        //     scan 'songdata', {COLUMNS => ['data:title'], FILTER => "SingleColumnValueFilter('data', 'title', = , 'regexstring:Total.*Water')"}

        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(
                this.FAMILY,
                Bytes.toBytes("user_id"),
                CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes(Long.parseLong(user_id))
        );
        list.addFilter(filter1);


        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(
                this.FAMILY,
                Bytes.toBytes("create_at"),
                CompareFilter.CompareOp.EQUAL,
                Bytes.toBytes(Long.parseLong(time))
        );
        list.addFilter(filter2);
        Scan s = new Scan();
        s.addColumn(this.FAMILY, this.USER_ID);
        s.addColumn(this.FAMILY, this.CREATE_AT);
        s.addColumn(this.FAMILY, this.TEXT);
        s.addColumn(this.FAMILY, this.SCORE);
        s.setFilter(list);
        ResultScanner scanner = null;
        ArrayList<TweetStructure> ret = null;
        try {
            scanner = table.getScanner(s);
            // Scanners return Result instances.
            // Now, for the actual iteration. One way is to use a while loop like so:
            for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
                // print out the row we found and the columns we were looking for

                byte[] id = rr.getRow();
                byte[] uid = rr.getValue(this.FAMILY, this.USER_ID);
                byte[] t = rr.getValue(this.FAMILY, this.CREATE_AT);
                byte[] text = rr.getValue(this.FAMILY, this.TEXT);
                byte[] score = rr.getValue(this.FAMILY, this.SCORE);
                TweetStructure tweetStructure = new TweetStructure(
                        Bytes.toLong(id), Bytes.toLong(uid), Bytes.toLong(t),
                        Bytes.toString(text), Bytes.toInt(score));

                if(ret == null){
                    ret = new ArrayList<TweetStructure>();
                }
                ret.add(tweetStructure);
                System.out.println(tweetStructure);
                System.out.println("----");
            }

        }catch (Exception e){
            e.printStackTrace();
        }
        finally {
            if(scanner != null) {
                scanner.close();
            }
            return ret;
        }
    }


    public Put create_put(String id, String userId, String time, String text, String score){
        Put p = new Put(Bytes.toBytes(Long.parseLong(id)));
        p.add(this.FAMILY, this.USER_ID, Bytes.toBytes(Long.parseLong(userId)));
        p.add(this.FAMILY, this.CREATE_AT, Bytes.toBytes(Long.parseLong(time)));
        p.add(this.FAMILY, this.TEXT, Bytes.toBytes(text));
        p.add(this.FAMILY, this.SCORE, Bytes.toBytes(Integer.parseInt(score)));
        return p;
    }

    public void recordCsvFile(String fileName) {

        FileReader fileReader = null;

        CSVParser csvFileParser = null;

        //Create the CSVFormat object with the header mapping
        CSVFormat csvFileFormat = CSVFormat.newFormat(',')
                .withEscape(null)
                .withQuote('"')
                .withRecordSeparator('\n');

        try {

            //Create a new list of student to be filled by CSV file data
            fileReader = new FileReader(fileName);

            //initialize CSVParser object
            csvFileParser = new CSVParser(fileReader, csvFileFormat);

            //Get a list of CSV file records
            List csvRecords = csvFileParser.getRecords();
            //Read the CSV file records starting from the second record to skip the header
            ArrayList<Put> putList = new ArrayList<Put>();
            for (int i = 0; i < csvRecords.size(); i++) {
                final CSVRecord record = (CSVRecord) csvRecords.get(i);
                assert record.size() == 5;
                Put p = create_put(record.get(0), record.get(1), record.get(2), record.get(3), record.get(4));
                putList.add(p);
            }
            table.put(putList);
        }
        catch (Exception e) {
            System.out.println("Error in CsvFileReader");
            e.printStackTrace();
        } finally {
            try {
                fileReader.close();
                csvFileParser.close();
            } catch (IOException e) {
                System.out.println("Error while closing fileReader/csvFileParser");
                e.printStackTrace();
            }
        }

    }
}
