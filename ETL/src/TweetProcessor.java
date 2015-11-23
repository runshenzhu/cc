import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


/**
 * Created by omega on 10/24/15.
 * Modified by jessesleep on 11/01/15.
 */

/**
 * TweetProcessor extracts sentiment score and generates censored text
 * given the tweet's original text field
 */

public class TweetProcessor {
    private Preprocessor preprocessor;

    private static final long startTimestamp = startTimestampSetter();
    private static final int secondInDay = 3600 * 24;

    private static long startTimestampSetter() {
        Date date = null;
        try {
            date = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z")
                    .parse("Sun, 01 Jan 2006 00:00:00 GMT");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date.getTime();
    }

    private static boolean isLetterOrDigit(char c) {
        return (c >= 'a' && c <= 'z') ||
                (c >= 'A' && c <= 'Z') ||
                (c >= '0' && c <= '9');
    }

    public static class CensorSentimentResult {
        public String censoredText;
        public Integer score;
        public CensorSentimentResult(String censoredText, Integer score) {
            this.censoredText = censoredText;
            this.score = score;
        }
    }

    /**
     * Round a skewed timestamp (in second offset) to day
     * @param skewedTimestamp
     * @return
     */
    public static int skewedTimestampRoundToDay( int skewedTimestamp ){
        return skewedTimestamp / secondInDay;
    }

    /**
     * Extract a local hashtag list from a tweet stucture
     * @param tweet
     * @return
     */
    public static ArrayList<HashtagStructure> getHashtagStruct( TweetStructure tweet ){
        ArrayList<HashtagStructure> hashtagList = new ArrayList<HashtagStructure>();

        // In case of duplicate hash tag in a single tweet
        Map<String, HashtagStructure> hashtagLocalMap = new HashMap<String, HashtagStructure>();

        for( String hashtag : tweet.hashtags ){
            if( hashtagLocalMap.containsKey(hashtag) ){
                hashtagLocalMap.get(hashtag).count += 1;
                continue;
            } else{
                HashtagStructure hashtagStructure = new HashtagStructure(
                        hashtag, TweetProcessor.skewTimeStamp(tweet.timestamp),
                        tweet.userId, tweet.text);
                // Add to list
                hashtagLocalMap.put(hashtag, hashtagStructure);
            }
        }

        for( Map.Entry<String, HashtagStructure> entry : hashtagLocalMap.entrySet() ){
            hashtagList.add(entry.getValue());
        }
        return hashtagList;
    }

    /**
     * Generate refined tweet for Q2Q3
     * @param origTweet
     * @return
     */
    public Q2Q3TweetStructure refineTweetStructure( TweetStructure origTweet ) {
        Q2Q3TweetStructure q2Q3TweetStructure = new Q2Q3TweetStructure();
        q2Q3TweetStructure.id = origTweet.id;
        q2Q3TweetStructure.userId = origTweet.userId;
        q2Q3TweetStructure.skewedTimestamp = TweetProcessor.skewTimeStamp(origTweet.timestamp);

        TweetProcessor.CensorSentimentResult censorSentimentResult = handleText(origTweet.text);
        q2Q3TweetStructure.censoredText = censorSentimentResult.censoredText;
        q2Q3TweetStructure.sentimentScore = censorSentimentResult.score;
        q2Q3TweetStructure.impactScore = q2Q3TweetStructure.sentimentScore * (origTweet.follwersCount+1);
        return q2Q3TweetStructure;
    }

    /**
     * Calculate the skewed time stamp.
     * @param timestamp
     * @return
     */
    public static int skewTimeStamp( long timestamp ){
        return (int)((timestamp - startTimestamp)/1000);
    }


    /**
     * Translate skewTime into date string
     * @param skewTime
     * @return
     */
    public static String skewedTime2Date( long skewTime ){
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        format.setTimeZone(TimeZone.getTimeZone("GMT"));
        long timeInMs = (skewTime * 1000) + TweetProcessor.startTimestamp;
        Date date = new Date(timeInMs);
        return format.format(date);
    }

    /**
     * Shard by string type value
     * @param shardCount
     * @param str
     * @return
     */
    public static int shardByStr( int shardCount, String str ){
        int hashVal = str.hashCode();
        if( hashVal < 0 ){
            hashVal *= -1;
        }
        return hashVal % shardCount;
    }

    /**
     * Calculate the shard of tweets
     * @return
     */
    public static int shardByLongId(int shardCount, long userId ){
        return shardByStr(shardCount, String.valueOf(userId));
    }

    /**
     * Extract the original tweet stucture from the given json output
     * @param line
     * @return
     */
    public static TweetStructure extractTweetStructure( String line ){
        JsonParser parser = new JsonParser();
        TweetStructure ts = new TweetStructure();
        String timeString;
        try {
            JsonObject json = (JsonObject) parser.parse(line);
            ts.id = json.get("id").getAsLong();
            ts.userId = json.get("user").getAsJsonObject().get("id").getAsLong();
            ts.follwersCount = json.get("user").getAsJsonObject().get("followers_count").getAsInt();

            JsonArray hashtags = json.get("entities").getAsJsonObject().get("hashtags").getAsJsonArray();
            for(JsonElement hashtag : hashtags){
                ts.hashtags.add(hashtag.getAsJsonObject().get("text").getAsString());
            }

            ts.text = json.get("text").getAsString();
            timeString = json.get("created_at").getAsString();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

        if (timeString == null)
            return null;
        DateFormat format = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
        Date date = null;
        try {
            date = format.parse(timeString);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        // Move the start data check to the front end, for ETL, we reserve all tweets
        if (date == null /*|| date.before(startDate)*/)
            return null;

    /*
    DateFormat outFormat = new SimpleDateFormat("yyyy-MM-dd+HH:mm:ss");
    outFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
    ts.timestamp = outFormat.format(date);
    */
        ts.timestamp = date.getTime();
        return ts;
    }

    /**
     * Constructor
     * @param sentimentBr
     * @param bannedWordBr
     */
    public TweetProcessor(BufferedReader sentimentBr,
                          BufferedReader bannedWordBr) {
        preprocessor = new Preprocessor(sentimentBr, bannedWordBr);
    }


    private CensorSentimentResult updateScore(String word) {
        String lowerWord = word.toLowerCase();
        Integer score = 0;

        // Get sentiment score of the word
        if (preprocessor.sentiment.containsKey(lowerWord)) {
            score = preprocessor.sentiment.get(lowerWord);
        }

        // Update word if it is a censored word
        String censoredText = "";
        if (preprocessor.bannedWords.contains(lowerWord)) {
            censoredText += word.charAt(0);
            for (int i = 0; i < word.length() - 2; ++i)
                censoredText += '*';
            censoredText += word.charAt(word.length() - 1);
        } else {
            censoredText += word;
        }

        return new CensorSentimentResult(censoredText, score);
    }

    public CensorSentimentResult handleText(String text) {
        String word = "";
        String censoredText = "";
        Integer sentiment = 0;
        boolean flag = false;

        for (int i = 0; i < text.length(); ++i) {
            char ch = text.charAt(i);
            if (isLetterOrDigit(ch)) {
                flag = true;
                word += ch;
            } else {
                // If there is no word to consume
                if (!flag) {
                    censoredText += ch;
                    continue;
                }
                CensorSentimentResult p = updateScore(word);
                censoredText += p.censoredText;
                sentiment += p.score;
                censoredText += ch;

                // Reset word
                word = "";
                flag = false;
            }
        }

        // Add last word
        if (flag) {
            CensorSentimentResult p = updateScore(word);
            censoredText += p.censoredText;
            sentiment += p.score;
        }
        return new CensorSentimentResult(censoredText, sentiment);
    }
}
