import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedReader;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;


/**
 * Created by omega on 10/24/15.
 */
public class TweetProcessor {
  private Preprocessor preprocessor;
  private static final Date startDate = startDateSetter();

  private static Date startDateSetter() {
    Date date = null;
    try {
      date = new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z")
          .parse("Sun, 20 Apr 2014 00:00:00 GMT");
    } catch (ParseException e) {
      e.printStackTrace();
    }
    return date;
  }

  public TweetProcessor(BufferedReader sentimentBr,
                        BufferedReader bannedWordBr) {
    preprocessor = new Preprocessor(sentimentBr, bannedWordBr);
  }
  private class TweetMetadata {
    public String censoredText;
    public Integer score;
    public TweetMetadata(String censoredText, Integer score) {
      this.censoredText = censoredText;
      this.score = score;
    }
  }

  private boolean isLetterOrDigit(char c) {
    return (c >= 'a' && c <= 'z') ||
        (c >= 'A' && c <= 'Z') ||
        (c >= '0' && c <= '9');
  }

  private TweetMetadata updateScore(String word) {
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

    return new TweetMetadata(censoredText, score);
  }

  private TweetMetadata handleText(String text) {
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
        TweetMetadata p = updateScore(word);
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
      TweetMetadata p = updateScore(word);
      censoredText += p.censoredText;
      sentiment += p.score;
    }
    return new TweetMetadata(censoredText, sentiment);
  }
  public TweetStructure handleLine(String line) {
    JsonParser parser = new JsonParser();
    TweetStructure ts = new TweetStructure();
    String text;
    try {
      JsonObject json = (JsonObject) parser.parse(line);
      ts.id = json.get("id").getAsLong();
      ts.userId = json.get("user").getAsJsonObject().get("id").getAsLong();
      text = json.get("text").getAsString();
      ts.timestamp = json.get("created_at").getAsString();
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }

    DateFormat format = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");
    Date date = null;
    try {
      date = format.parse(ts.timestamp);
    } catch (ParseException e) {
      e.printStackTrace();
    }

    if (date == null || date.before(startDate))
      return null;

    DateFormat outFormat = new SimpleDateFormat("yyyy-MM-dd+HH:mm:ss");
    outFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
    ts.timestamp = outFormat.format(date);

    if (text == null)
      return null;
    TweetMetadata p = handleText(text);

    ts.score = p.score;
    ts.censoredText = p.censoredText;
    return ts;
  }
}
