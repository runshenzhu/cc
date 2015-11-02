import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Created by jessesleep on 11/1/15.
 */
public class Json2JsonTester2 {
    public static void main( String [] args ) throws IOException {
        if( args.length != 2 ){
            System.out.println("Json2JsonTester <in> <out>");
            System.exit(1);
        }

        BufferedReader fin = null;
        FileWriter fout = null;
        try {
            fin = new BufferedReader(new FileReader(args[0]));
            fout = new FileWriter(args[1]);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }

        String line;
        while( (line = fin.readLine()) != null ){
            TweetStructure tweet = new TweetStructure(line);
            fout.write(tweet.toString());
        }

        return;
    }
}
