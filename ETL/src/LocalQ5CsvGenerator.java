import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by jessesleep on 11/23/15.
 */
public class LocalQ5CsvGenerator {
    /**
     * Shard by string type value
     * @param shardCount
     * @param str
     * @return
     */
    public static int shardStr( int shardCount, String str ){
        int hashVal = str.hashCode();
        if( hashVal < 0 ){
            hashVal *= -1;
        }
        return hashVal % shardCount;
    }

    public static void main( String [] args ) throws IOException{
        if( args.length != 3 ){
            System.out.println("java LocalQ5CsvGenerator <input_file> <output_prefix> <shard_count>");
            System.exit(1);
        }
        BufferedReader br = new BufferedReader( new FileReader(args[0]));
        ArrayList<FileWriter> fileOuts = new ArrayList<FileWriter>();
        int shards = Integer.parseInt(args[2]);
        long totalCount = 0;

        for( int i = 0; i < shards; ++i ){
            fileOuts.add( new FileWriter(args[1]+i));
        }

        String line;

        while( (line = br.readLine()) != null ){
            int index = line.indexOf('\t');
            String userIdStr = line.substring(0, index);
            totalCount += Long.valueOf(line.substring(index+1));

            fileOuts.get(shardStr(shards, userIdStr)).write(userIdStr+","+totalCount);
        }

        for( int i = 0; i < shards; ++i ){
            fileOuts.get(i).close();
        }
    }
}
