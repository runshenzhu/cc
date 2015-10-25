import org.apache.commons.lang3.StringEscapeUtils;

/**
 * Created by omega on 10/24/15.
 */
public class TweetStructure {
  public long id;
  public long userId;
  public String timestamp;
  public String censoredText;
  public Integer score;

  public TweetStructure(long id, long userId,
                        String timestamp, String censoredText, Integer score) {
    this.id = id;
    this.userId = userId;
    this.timestamp = timestamp;
    this.censoredText = censoredText;
    this.score = score;
  }

  public TweetStructure() {}

  public String toString() {
    return id + "," + userId + "," + timestamp + "," +
        StringEscapeUtils.escapeCsv(censoredText) + "," + score;
  }
}
