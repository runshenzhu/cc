package com.obgun.frontend;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by Jianhong Li on 11/8/15.
 */
public class SQLHandler {
  protected static String mySqlUrl;
  protected static String mySqlUser;
  protected static String mySqlPasswd;
  //    private static String connectionRequest;
  private static final Date startDate = startDateSetter();

  private static ComboPooledDataSource[] cpds;
  private static final int SHARDING_COUNT = 6;

  /**
   * Set up the mySql client
   * @param urls
   * @param user
   * @param passwd
   */
  protected static boolean setMySql(String[] urls, String user, String passwd) {
    cpds = new ComboPooledDataSource[SHARDING_COUNT];
    for (int i = 0; i < SHARDING_COUNT; ++i) {
      SQLHandler.mySqlUrl = urls[i];
      SQLHandler.mySqlUser = user;
      SQLHandler.mySqlPasswd = passwd;

      cpds[i] = new ComboPooledDataSource();
      try {
        cpds[i].setDriverClass("com.mysql.jdbc.Driver");
        cpds[i].setJdbcUrl("jdbc:mysql://" + mySqlUrl +
            "/twitter?useUnicode=true&characterEncoding=UTF-8");
        cpds[i].setUser(mySqlUser);
        cpds[i].setPassword(mySqlPasswd);

        cpds[i].setInitialPoolSize(200);
        cpds[i].setMinPoolSize(200);
        cpds[i].setAcquireIncrement(5);
        cpds[i].setMaxPoolSize(200);
      } catch (PropertyVetoException e) {
        System.out.println(e);
        e.printStackTrace();
        return false;
      }
    }

    return true;
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


  public static String skewedTime2Date( long skewTime ){
    final DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
    format.setTimeZone(TimeZone.getTimeZone("GMT"));
    final long timeInMs = (skewTime * 1000) + TweetProcessor.startTimestamp;
    final Date date = new Date(timeInMs);
    return format.format(date);
  }
  /**
   * Issue an SQL request to the backend
   * @return
   */
  public static String getSqlAnswerQ2( String userId, String ts ){
    Connection conn = null;
    Date date = null;
    try {
      final DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      format.setTimeZone(TimeZone.getTimeZone("GMT"));
      date = format.parse(ts);
    } catch (ParseException e) {
      e.printStackTrace();
    }

    // Check whether the requested timestamp is valid or not
    if( date == null || date.before(startDate) ){
      return null;
    }
    ResultSet rs = null;
    Statement stmt = null;
    final StringBuilder answer = new StringBuilder();
    final int skewedTimestamp = TweetProcessor.skewTimeStamp(date.getTime());
    int hashCode = userId.hashCode();
    if (hashCode < 0)
      hashCode *= -1;
    hashCode %= SHARDING_COUNT;

    // Request a new connection for every request
    try {
      conn = cpds[hashCode].getConnection();

      //System.out.println("Connect to SQL");

      // May cause problem
      conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);

      stmt = conn.createStatement();
      final String query = "SELECT id, sentiment, text FROM tweets" +
          " WHERE user_id=" + userId + " AND create_at=" + skewedTimestamp +" ORDER BY id;";
      // System.out.println(query);
      rs = stmt.executeQuery(query);

      //System.out.println("Query executed: " + query);

      // Parse the result set
      while( rs.next() ){
        long tweetId = rs.getLong("ID");
        int sentiment = rs.getInt("SENTIMENT");
        String text = rs.getString("TEXT");

        answer.append(tweetId);
        answer.append(":");
        answer.append(sentiment);
        answer.append(":");
        answer.append(text);
        answer.append("\n");
      }



    } catch (SQLException ex) {
      System.out.println("SQLException: " + ex.getMessage());
      System.out.println("SQLState: " + ex.getSQLState());
      System.out.println("VendorError: " + ex.getErrorCode());
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

      if( conn != null ){
        try {
          conn.close();
        } catch (SQLException e) {} // ignore
      }
    }
    return answer.toString();
  }
}
