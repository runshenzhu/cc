import org.apache.commons.lang3.StringEscapeUtils;

/**
 * Created by omega on 10/24/15.
 */
public class TweetStructure {
  public long id;
  public long userId;
  public long timestamp;
  public String censoredText;
  public int score;

  public TweetStructure(long id, long userId,
                        long timestamp, String censoredText, Integer score) {
    this.id = id;
    this.userId = userId;
    this.timestamp = timestamp;
    this.censoredText = censoredText;
    this.score = score;
  }

  public TweetStructure() {}

  public String toEscapeString() {
    return id + "," + userId + "," + timestamp + "," +
        StringEscapeUtils.escapeCsv(censoredText) + "," + score;
  }
  
  public String toString(){
        return "id: " + this.id + " userId: " + this.userId +
                " timestamp: " + timestamp +
                " text: " + this.censoredText +
                " score: " + this.score;
    }
}
