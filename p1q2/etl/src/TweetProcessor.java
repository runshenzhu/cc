import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


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
//    private static final Date startDate = startDateSetter();
//
//    private static Date startDateSetter() {
//        Date date = null;
//        try {
//            date = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z")
//                    .parse("Sun, 20 Apr 2014 00:00:00 GMT");
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
//        return date;
//    }

    public TweetProcessor(BufferedReader sentimentBr,
                          BufferedReader bannedWordBr) {
        preprocessor = new Preprocessor(sentimentBr, bannedWordBr);
    }

    private boolean isLetterOrDigit(char c) {
        return (c >= 'a' && c <= 'z') ||
                (c >= 'A' && c <= 'Z') ||
                (c >= '0' && c <= '9');
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
