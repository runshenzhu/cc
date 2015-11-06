import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;


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
        return skewedTimestamp % secondInDay;
    }

    /**
     * Extract a local hashtag list from a tweet stucture
     * @param tweet
     * @return
     */
    public static ArrayList<HashtagStructure> getHashtagStruct( TweetStructure tweet ){
        ArrayList<HashtagStructure> hashtagList = new ArrayList<HashtagStructure>();

        // In case of duplicate hash tag in a single tweet
        HashMap<String, HashtagStructure> hashtagLocalMap = new HashMap<String, HashtagStructure>();

        for( String hashtag : tweet.hashtags ){
            if( hashtagLocalMap.containsKey(hashtag) ){
                // TODO: handle duplicate hashtag in the same tweet
                // HashtagStructure hashtagStruct = hashtagLocalMap.get(hashtag);
                // hashtagStruct.count += 1;
                continue;
            } else{
                HashtagStructure hashtagStructure = new HashtagStructure();
                hashtagStructure.hashtag = hashtag;
                hashtagStructure.skewedTimestamp = TweetProcessor.skewTimeStamp(tweet.timestamp);
                hashtagStructure.count = 1;
                hashtagStructure.userList.add(tweet.userId);
                hashtagStructure.sourceText = tweet.text;
                // Add to list
                hashtagList.add(hashtagStructure);
                hashtagLocalMap.put(hashtag, hashtagStructure);
            }
        }

        return hashtagList;
    }

    /**
     * Generate refined tweet for Q2Q3
     * @param origTweet
     * @return
     */
    public RefinedTweetStructure refineTweetStructure( TweetStructure origTweet ) {
        RefinedTweetStructure refinedTweetStructure = new RefinedTweetStructure();
        refinedTweetStructure.id = origTweet.id;
        refinedTweetStructure.userId = origTweet.userId;
        refinedTweetStructure.skewedTimestamp = TweetProcessor.skewTimeStamp(origTweet.timestamp);

        TweetProcessor.CensorSentimentResult censorSentimentResult = handleText(origTweet.text);
        refinedTweetStructure.censoredText = censorSentimentResult.censoredText;
        refinedTweetStructure.sentimentScore = censorSentimentResult.score;
        refinedTweetStructure.impactScore = refinedTweetStructure.sentimentScore * (origTweet.follwersCount+1);
        return refinedTweetStructure;
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
    public static int shardByUserId( int shardCount, long userId ){
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

//    public TweetStructure handleLine(String line) {
//        JsonParser parser = new JsonParser();
//        TweetStructure ts = new TweetStructure();
//        String text;
//        String timeString;
//        try {
//            JsonObject json = (JsonObject) parser.parse(line);
//            ts.id = json.get("id").getAsLong();
//            ts.userId = json.get("user").getAsJsonObject().get("id").getAsLong();
//            ts.follwersCount = json.get("user").getAsJsonObject().get("follwers_count").getAsInt();
//
//            JsonArray hashtags = json.get("entities").getAsJsonObject().get("hashtags").getAsJsonArray();
//            for(JsonElement hashtag : hashtags){
//                ts.hashtags.add(hashtag.getAsJsonObject().get("text").getAsString());
//            }
//
//            text = json.get("text").getAsString();
//            timeString = json.get("created_at").getAsString();
//        } catch (Exception e) {
//            e.printStackTrace();
//            return null;
//        }
//
//        if (timeString == null)
//            return null;
//        DateFormat format = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
//        Date date = null;
//        try {
//            date = format.parse(timeString);
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
//
//        // Move the start data check to the front end, for ETL, we reserve all tweets
//        if (date == null /*|| date.before(startDate)*/)
//            return null;
//
//    /*
//    DateFormat outFormat = new SimpleDateFormat("yyyy-MM-dd+HH:mm:ss");
//    outFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
//    ts.timestamp = outFormat.format(date);
//    */
//        ts.timestamp = date.getTime();
//
//        if (text == null)
//            return null;
//        CensorSentimentResult p = handleText(text);
//
//        ts.score = p.score;
//        ts.censoredText = p.censoredText;
//        return ts;
//    }
}
