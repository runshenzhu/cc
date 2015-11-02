import java.io.*;

/**
 * Created by jessesleep on 11/1/15.
 */
public class Json2JsonTester {
    public static void main( String [] args ) throws IOException{
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
        int count = 0;
        while( (line = fin.readLine()) != null ){
            count ++;
            TweetStructure tweet = TweetFilter.extractTweetStructure(line);
            if( tweet == null ){
                System.out.println("Detect a bad line");
                continue;
            }
            String json = tweet.toJsonLine();
            fout.write(json);
        }

        System.out.println("Parse "+count+" lines");
        fout.close();
        fin.close();
        return;
    }
}
