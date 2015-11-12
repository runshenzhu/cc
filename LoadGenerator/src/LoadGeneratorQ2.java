import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.impl.client.HttpClientBuilder;
import sun.net.util.URLUtil;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by jessesleep on 11/11/15.
 */
public class LoadGeneratorQ2 {
    protected static String targetDns;
    protected static AtomicInteger responseCount = new AtomicInteger(0);
    protected static ExecutorService excutors = Executors.newFixedThreadPool(16);
    protected static String skewedTime2Date( long skewTime ){
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd+HH:mm:ss");
        format.setTimeZone(TimeZone.getTimeZone("GMT"));
        long timeInMs = (skewTime * 1000) + TweetProcessor.startTimestamp;
        Date date = new Date(timeInMs);
        return format.format(date);
    }

    public static class RequestSender implements Runnable {
        private Q2Q3TweetStructure tweet;

        public RequestSender(Q2Q3TweetStructure tweet){
            this.tweet = tweet;
        }

        public void run(){
            try {
                String get = "http://"+targetDns+"/q2?userid="+tweet.userId+"&tweet_time="+skewedTime2Date(tweet.skewedTimestamp);
                HttpGet httpGet = new HttpGet(get);
                HttpClient client = HttpClientBuilder.create().build();
                HttpResponse response = client.execute(httpGet);

                if( response.getStatusLine().getStatusCode() == 200 ){
                    responseCount.getAndAdd(1);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class Watcher implements Runnable {
        private int count = 0;
        public void run(){
            while( true ){
                try {
                    Thread.sleep(1000*30);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                int currentCount = responseCount.get();
                System.out.println("RPS: " + (currentCount-count)/30 );
                count = currentCount;
            }
        }
    }

    public static void main(String [] args){
        if( args.length != 2 ){
            System.out.println("java LoadGeneratorQ2 <inputJsaon> <targetDNS>");
            System.exit(1);
        }
        BufferedReader fin = null;
        targetDns = args[1];
        while(true) {
            try {
                fin = new BufferedReader(new FileReader(args[0]));
                String line;

                Thread watcherThread = new Thread(new Watcher());
                watcherThread.start();
                while ((line = fin.readLine()) != null) {
                    Q2Q3TweetStructure tweet = new Q2Q3TweetStructure(line);
                    Runnable thread = new RequestSender(tweet);
                    excutors.execute(thread);
                }

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if( fin != null ) {
                    try {
                        fin.close();
                    } catch (IOException e) {}
                }
            }
        }
    }
}
