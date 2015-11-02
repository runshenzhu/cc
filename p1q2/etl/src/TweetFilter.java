import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by jessesleep on 11/1/15.
 */
public class TweetFilter {
    /**
     * Extract a TweetStructure form a line of Json file.
     * Return null if the json line is in bad format
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
}
