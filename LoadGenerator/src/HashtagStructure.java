import com.google.gson.Gson;
import org.apache.commons.lang3.StringEscapeUtils;

import java.util.ArrayList;

/**
 * Created by jessesleep on 11/5/15.
 */
public class HashtagStructure implements Comparable {
    public String hashtag;
    public int skewedTimestamp;
    int count;
    ArrayList<Long> userList;
    public String sourceText;

    public HashtagStructure(){
        userList = new ArrayList<Long>();
    }

    @Override
    public int compareTo(Object that){
        int hashtagDiff = this.hashtag.compareTo(((HashtagStructure)that).hashtag);
        if( hashtagDiff == 0 ){
            int countDiffRev = ((HashtagStructure) that).count - this.count;
            if( countDiffRev == 0 ){
                return this.skewedTimestamp - ((HashtagStructure) that).skewedTimestamp;
            }
            else{
                return countDiffRev;
            }
        }
        else{
            return hashtagDiff;
        }
    }

    public HashtagStructure(String jsonLine ){
        Gson gson = new Gson();
        HashtagStructure temp = gson.fromJson(jsonLine, this.getClass());
        this.hashtag = temp.hashtag;
        this.skewedTimestamp = temp.skewedTimestamp;
        this.count = temp.count;
        this.userList = temp.userList;
        this.sourceText = temp.sourceText;
    }

    public HashtagStructure(String hashtag, int skewedTimestamp,
                            Long userid, String sourceText){
        this.hashtag = hashtag;
        this.skewedTimestamp = skewedTimestamp;
        this.count = 1;
        this.userList = new ArrayList<Long>();
        this.userList.add(userid);
        this.sourceText = sourceText;
    }


    public String toJsonLine(){
        Gson gson = new Gson();
        String json = gson.toJson(this);
        return json;
    }

    /**
     * Output in a csv format. Multi line....
     * @return
     */
    public String toEscapeString() {
        StringBuilder userListStrBuilder = new StringBuilder();
        for( int i = 0; i < userList.size(); ++i ){
            if( i == 0 ){
                userListStrBuilder.append(userList.get(i));
            } else {
                userListStrBuilder.append(",").append(userList.get(i));
            }
        }

        return StringEscapeUtils.escapeCsv(hashtag) + "," + skewedTimestamp + "," +
                count + "," + StringEscapeUtils.escapeCsv(userListStrBuilder.toString()) + "," +
                StringEscapeUtils.escapeCsv(sourceText);
    }
}
