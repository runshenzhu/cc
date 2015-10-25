import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by omega on 10/24/15.
 */
public class Main {
  public static void main(String[] args) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader("part-00001"));

    BufferedReader sentimentBr =
        new BufferedReader(new FileReader("afinn.txt"));
    BufferedReader bannedWordBr =
        new BufferedReader(new FileReader("banned.txt"));
    try {
      TweetProcessor processor = new TweetProcessor(sentimentBr, bannedWordBr);
      String line = br.readLine();
      while (line != null) {
        System.out.println(processor.handleLine(line));
        line = br.readLine();
      }
    } finally {
      br.close();
      sentimentBr.close();
      bannedWordBr.close();
    }
  }
}
