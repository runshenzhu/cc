/**
 * Created by jessesleep on 11/1/15.
 */
public class CensorSentimentResult {
    public String censoredText;
    public Integer score;
    public CensorSentimentResult(String censoredText, Integer score) {
        this.censoredText = censoredText;
        this.score = score;
    }
}