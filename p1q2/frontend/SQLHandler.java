/**
 * Created by jessesleep on 10/25/15.
 */
import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

// TODO: Compile the query in advance --> Make it in the backend

public class SQLHandler {
    protected static String mySqlUrl;
    protected static String mySqlUser;
    protected static String mySqlPasswd;
    private static String connectionRequest;
    private static final Date startDate = startDateSetter();

    /**
     * Set up the mySql client
     * @param url
     * @param user
     * @param passwd
     */
    protected static void setMySql(String url, String user, String passwd ){
        SQLHandler.mySqlUrl = url;
        SQLHandler.mySqlUser = user;
        SQLHandler.mySqlPasswd = passwd;
        SQLHandler.connectionRequest = "jdbc:mysql://"+SQLHandler.mySqlUrl +
                "/twitter?user="+SQLHandler.mySqlUser +
                "&password="+SQLHandler.mySqlPasswd;
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Set the start date
     * @return
     */
    private static Date startDateSetter(){
        Date date = null;
        try {
           date =  new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z")
                    .parse("Sun, 20 Apr 2014 00:00:00 GMT");
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return date;
    }

    /**
     * Issue an SQL request to the backend
     * @return
     */
    public static String getSqlAnswers( String userid, String ts ){
        Connection conn;
        Date timestamp = null;
        try {
            DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            timestamp = format.parse(ts);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        // Check whether the requested timestamp is valid or not
        if( timestamp == null || timestamp.before(startDate) ){
            return null;
        }
        ResultSet rs = null;
        Statement stmt = null;
        String answer = "";

        // Request a new connection for every request
        try {
            conn = DriverManager.getConnection(SQLHandler.connectionRequest);
            stmt = conn.createStatement();

            rs = stmt.executeQuery("SELECT id, sentiment, text FROM tweets" +
                    " WHERE user_id=" + userid + " AND create_at=" + timestamp.getTime()+" ORDER BY id;");

            // Parse the result set
            while( rs.next() ){
                long tweetId = rs.getLong("ID");
                int sentiment = rs.getInt("SENTIMENT");
                String text = rs.getString("TEXT");

                answer += (""+tweetId+":"+sentiment+":"+text+"\n");
            }

        } catch (SQLException ex) {
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
            answer = null;
        } finally {
            if( rs != null ) {
                try {
                    rs.close();
                } catch (SQLException e) {} // ignore
            }
            if( stmt != null ){
                try{
                    stmt.close();
                } catch( SQLException e) {} // ignore
            }
        }

        return answer;
    }

}
