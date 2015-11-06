import com.google.gson.Gson;

import java.util.ArrayList;

/**
 * Created by jessesleep on 11/5/15.
 */
public class HashtagStructure {
    public String hashtag;
    public int skewedTimestamp;
    int count;
    ArrayList<Long> userList;
    public String sourceText;

    public HashtagStructure(){
        userList = new ArrayList<Long>();
    }

    public HashtagStructure(String jsonLine ){
        Gson gson = new Gson();
        HashtagStructure temp = gson.fromJson(jsonLine, this.getClass());
        this.hashtag = temp.hashtag;
        this.skewedTimestamp = temp.skewedTimestamp;
        this.count = temp.count;
        this.userList = temp.userList;
    }

    public String toJsonLine(){
        Gson gson = new Gson();
        String json = gson.toJson(this);
        return json;
    }
}
