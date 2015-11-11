package com.obgun.frontend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.rest.client.Client;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

final public class HbaseHandler {
    // Column name
    // TODO: change table, colo and family name
    /*
    static final private byte[] TEXT = Bytes.toBytes("text");
    static final private byte[] SCORE = Bytes.toBytes("sentiment");
    */
    static final private byte[] Q4C = Bytes.toBytes("v");
    private static final byte[] TEXT = Bytes.toBytes("t");
    private static final byte[] SCORE = Bytes.toBytes("s");
    private static final byte[] IMPACT = Bytes.toBytes("i");
    // TODO: set the sheet name here !!!!!
    //private static final String TABLE = "twitter";
    private static final String TABLE = "tweet";
    private static final String TABLEQ4 = "hashegg";
    private static final byte[] FAMILY = Bytes.toBytes("o");

    private static String hbaseUrl;
    private static Client client;
    private static Configuration hbaseConfig;
    private static HConnection hConnection;
//    private static final DateFormat dateTimeformat = setDateTimeFormat();
//    private static final DateFormat dateFormat = setDateFormat();
//
//    private final static DateFormat setDateFormat(){
//        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
//        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
//        return dateFormat;
//    }
//
//    private final static DateFormat setDateTimeFormat(){
//        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
//        return dateFormat;
//    }

    final static class ImpactTweet implements Comparable {
        int skewedTimestamp;
        long tweetid;
        String text;
        int impactScore;

        public ImpactTweet(int skewedTimestamp, long tweeid, String text, int impactSocre){
            this.skewedTimestamp = skewedTimestamp;
            this.tweetid = tweeid;
            this.text = text;  // TODO: replace newline, fuck TA
            this.impactScore = impactSocre;
        }

        @Override
        public int compareTo(Object that){
            if( this.impactScore != ((ImpactTweet)that).impactScore ){
                return this.impactScore - ((ImpactTweet) that).impactScore;
            }
            else{
                int flag = (this.impactScore > 0 ) ? -1 : 1;
                return  (this.skewedTimestamp - ((ImpactTweet) that).skewedTimestamp) * flag;
            }
        }
    }

    final public static boolean setHbase( String url ){
        hbaseUrl = url;

        hbaseConfig = HBaseConfiguration.create();
        hbaseConfig.clear();
        hbaseConfig.addResource("hbase-site.xml");
        hbaseConfig.set("hbase.zookeeper.quorum", hbaseUrl);
        hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181");
        hbaseConfig.set("hbase.master", url + ":60010");
        try {
            HBaseAdmin.checkHBaseAvailable(hbaseConfig);
            hConnection = HConnectionManager.createConnection(hbaseConfig /*, Executors.newFixedThreadPool(200)*/);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
            return false;
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
            return false;
        } catch (IOException e ){
            e.printStackTrace();
            return false;
        }

        return true;
    }

    final public static String skewedTime2Date( long skewTime ){
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        format.setTimeZone(TimeZone.getTimeZone("GMT"));
        long timeInMs = (skewTime * 1000) + TweetProcessor.startTimestamp;
        Date date = new Date(timeInMs);
        return format.format(date);
    }

    final public static String getHbaseAnswerQ3( String userId, String startDate, String endDate, String nStr ){
        int startSkew, endSkew;
        try {
            DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            format.setTimeZone(TimeZone.getTimeZone("GMT"));
            startSkew = TweetProcessor.skewTimeStamp(format.parse(startDate + " 00:00:00").getTime());
            endSkew = TweetProcessor.skewTimeStamp(format.parse(endDate+" 23:59:59").getTime()) + 1;
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("uid: "+userId+" Start: "+startDate+" End: "+endDate+" n: "+nStr);
            return null;
        }

        int n;
        try {
            n = Integer.valueOf(nStr);
        } catch (NumberFormatException e) {
            e.printStackTrace();
            return null;
        }

        int hashCode = (userId).hashCode();
        byte [] startRow = Bytes.add(Bytes.toBytes(Long.parseLong(userId)),
                Bytes.toBytes(startSkew));
        byte [] endRow = Bytes.add(Bytes.toBytes(Long.parseLong(userId)),
                Bytes.toBytes(endSkew));
        // Score filter
        SingleColumnValueFilter impactFilter = new SingleColumnValueFilter(
                FAMILY, IMPACT, CompareFilter.CompareOp.NOT_EQUAL, Bytes.toBytes(0));

        Scan scan = new Scan(startRow, endRow);
        scan.setFilter(impactFilter);
        scan.setMaxVersions(3);
        scan.addColumn(FAMILY, IMPACT);
        scan.addColumn(FAMILY, TEXT);
        scan.setCacheBlocks(true);
        scan.setCaching(200);
        HTableInterface table = null;
        ArrayList<ImpactTweet> tweets = new ArrayList<ImpactTweet>();
        ResultScanner scanner = null;
        try {
            table = hConnection.getTable(TABLE);
            scanner = table.getScanner(scan);

            Result rr;
            while( (rr = scanner.next()) != null ){
                int skewTs = Bytes.toInt(Bytes.tail(rr.getRow(), 4));
                List<KeyValue> texts = rr.getColumn(FAMILY, TEXT);
                List<KeyValue> impacts = rr.getColumn(FAMILY, IMPACT);
                for( int i = 0; i < texts.size(); ++i ){
                    KeyValue impact = impacts.get(i);
                    KeyValue text = texts.get(i);
                    tweets.add(new ImpactTweet(skewTs, impact.getTimestamp(),
                            Bytes.toString(text.getValue()), Bytes.toInt(impact.getValue())));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(scanner != null){
                try{
                    scanner.close();
                }catch (Exception e){}
            }
        }

        Collections.sort(tweets);
        // Generate response
        StringBuilder positiveBuilder = new StringBuilder("Positive Tweets\n");
        StringBuilder negativeBuilder = new StringBuilder("\nNegative Tweets\n");
        int tweetCount = tweets.size();
        for( int i = 0; i < tweetCount && i < n; ++i ){
            ImpactTweet negTweet = tweets.get(i);
            ImpactTweet posTweet = tweets.get(tweetCount-i-1);
            if( negTweet.impactScore >= 0 && posTweet.impactScore <= 0 ){
                break;
            }
            if( negTweet.impactScore < 0 ){
                negativeBuilder.append(skewedTime2Date(negTweet.skewedTimestamp)).append(",")
                        .append(String.valueOf(negTweet.impactScore)).append(",")
                        .append(String.valueOf(negTweet.tweetid)).append(",")
                        .append(negTweet.text).append("\n");
            }
            if( posTweet.impactScore > 0 ) {
                positiveBuilder.append(skewedTime2Date(posTweet.skewedTimestamp)).append(",")
                        .append(String.valueOf(posTweet.impactScore)).append(",")
                        .append(String.valueOf(posTweet.tweetid)).append(",")
                        .append(posTweet.text).append("\n");
            }
        }
        return positiveBuilder.toString()+negativeBuilder.toString();
    }

    final public static String getHbaseAnswerQ2(String userId, String time){
        long timestamp;

        try {
            DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            format.setTimeZone(TimeZone.getTimeZone("GMT"));
            timestamp = format.parse(time).getTime();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("uid: "+userId+" Time: "+time);
            return null;
        }

        int skewedTimestamp = TweetProcessor.skewTimeStamp(timestamp);
        int hashCode = (userId).hashCode();

        byte [] row = Bytes.add(Bytes.toBytes(Long.parseLong(userId)),
                Bytes.toBytes(skewedTimestamp));
        /*
        byte[] row = Bytes.add(Bytes.toBytes(Long.parseLong(userId)),
                Bytes.toBytes(timestamp));
                */
        String res = "";


        // not hit
        Get get = new Get(row);
        HTableInterface table = null;
        try {
            //Create the CSVFormat object with the header mapping
            //table = new HTable(hbaseConfig, TABLE);
            table = hConnection.getTable(TABLE);
            get.setMaxVersions(3);
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

/*
    //scan
    final static String getHbaseAnswerQ4(String tag, String rank){
        int start = 0;
        int end = Integer.parseInt(rank);
        byte[] startRow = Bytes.add(Bytes.toBytes(tag),
                Bytes.toBytes(start));
        byte[] endRow = Bytes.add(Bytes.toBytes(tag),
                Bytes.toBytes(end));

        Scan scan = new Scan(startRow, endRow);
        scan.addColumn(FAMILY, Q4C);
        scan.setCacheBlocks(true);
        scan.setCaching(end);
        HTableInterface table = null;
        ResultScanner scanner = null;
        String ret = "";
        try{
            table = hConnection.getTable(TABLEQ4);
            scanner = table.getScanner(scan);

            Result rr;
            while( (rr = scanner.next()) != null ){
                // TODO: use string builder
                // TODO: delete \n
               ret += Bytes.toString(rr.getValue(FAMILY, Q4C)) + "\n";
            }
        }catch (Exception e){}
        finally {
            try{
                if (table != null){
                    table.close();
                }
            }catch (Exception e){}
            try {
                if (scanner != null){
                    scanner.close();
                }
            }catch (Exception e){}

            return ret;
        }
    }
*/
    // get
    final static String getHbaseAnswerQ4(String tag, String rank){
        byte[] rowkey = Bytes.add(Bytes.toBytes(tag.hashCode()), Bytes.toBytes(tag));
        Get get = new Get(rowkey);
        int end = Integer.parseInt(rank);
        for(int i = 0; i < end; i++){
            get.addColumn(FAMILY, Bytes.toBytes(i));
        }

        HTableInterface table = null;
        String ret = "";
        try{
            table = hConnection.getTable(TABLEQ4);

            Result rr = table.get(get);
            for(int i = 0; i < end; i++){
                ret += rr.getValue(FAMILY, Bytes.toBytes(i));
            }
        }catch (Exception e){}
        finally {
            try{
                if (table != null){
                    table.close();
                }
            }catch (Exception e){}
            return ret;
        }
    }

}