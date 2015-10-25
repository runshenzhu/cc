import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by omega on 10/24/15.
 */
public class Preprocessor {
  public Map<String, Integer> sentiment = new HashMap<String, Integer>();
  public Set<String> bannedWords = new HashSet<String>();

  private void initSentiment(BufferedReader br) throws IOException {
    String line = br.readLine();
    while (line != null) {
      String[] values = line.split("\t");
      sentiment.put(values[0], Integer.parseInt(values[1]));
      line = br.readLine();
    }
  }

  private void initBannedWords(BufferedReader br) throws IOException {
    String line = br.readLine();
    while (line != null) {
      String bannedWord = "";
      for (int i = 0; i < line.length(); ++i) {
        char ch = line.charAt(i);
        if (Character.isLetter(ch))
          bannedWord += (char)('a' + ((ch - 'a' + 13) % 26));
        else
          bannedWord += ch;
      }
      bannedWords.add(bannedWord);
      line = br.readLine();
    }
  }

  public Preprocessor(BufferedReader sentimentBr, BufferedReader bannedWordBr) {
    try {
      initSentiment(sentimentBr);
      initBannedWords(bannedWordBr);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
