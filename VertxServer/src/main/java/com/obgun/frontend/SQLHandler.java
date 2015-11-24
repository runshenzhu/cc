package com.obgun.frontend;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.beans.PropertyVetoException;
import java.sql.*;
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
  private static final int SHARDING_COUNT = 8;

  /**
   * Set up the mySql client
   * @param urls
   * @param user
   * @param passwd
   */
  protected static boolean setMySql(String[] urls, String user, String passwd) throws SQLException {
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

        cpds[i].setInitialPoolSize(100);
        cpds[i].setMinPoolSize(100);
        cpds[i].setAcquireIncrement(5);
        cpds[i].setMaxPoolSize(100);
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
    PreparedStatement q2Statement = null;
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

//      final String query = "SELECT id, sentiment, text FROM tweets" +
//          " WHERE user_id=" + userId + " AND create_at=" + skewedTimestamp +" ORDER BY id;";
      // System.out.println(query);

      q2Statement = conn.prepareStatement(
          "SELECT id, sentiment, text FROM tweets WHERE user_id = ? AND create_at = ? ORDER BY id"
      );
      q2Statement.setLong(1, Long.valueOf(userId));
      q2Statement.setInt(2, skewedTimestamp);
      rs = q2Statement.executeQuery();

//      rs = stmt.executeQuery(query);

      //System.out.println("Query executed: " + query);

      answer.append("Omegaga's Black Railgun,6537-0651-1730\n");
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
      if( q2Statement != null ){
        try{
          q2Statement.close();
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

  public static String getSqlAnswerQ3(String userId,
                               String startTs, String endTs, int n) {
    final DateFormat outputDateFormat =
        new SimpleDateFormat("yyyy-MM-dd");
    outputDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
    Connection conn = null;
    Date startDate = null;
    Date endDate = null;
    try {
      final DateFormat format = new SimpleDateFormat("yyyy-MM-dd");
      format.setTimeZone(TimeZone.getTimeZone("GMT"));
      startDate = format.parse(startTs);
      endDate = format.parse(endTs);
    } catch (ParseException e) {
      e.printStackTrace();
    }

    // Check whether the requested timestamp is valid or not
    if(startDate == null)
      return null;
    if(endDate == null)
      return null;
    ResultSet rsPos = null;
    ResultSet rsNeg = null;
    final StringBuilder answer = new StringBuilder();
    final int skewedStartDate = TweetProcessor.skewTimeStamp(startDate.getTime());
    final int skewedEndDate = TweetProcessor.skewTimeStamp(endDate.getTime());
    int hashCode = userId.hashCode();
    if (hashCode < 0)
      hashCode *= -1;
    hashCode %= SHARDING_COUNT;
    PreparedStatement q3PosStatement = null;
    PreparedStatement q3NegStatement = null;

    // Request a new connection for every request
    try {
      conn = cpds[hashCode].getConnection();

      //System.out.println("Connect to SQL");

      // May cause problem
      conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);

      q3PosStatement = conn.prepareStatement(
          "SELECT create_at, impact, id, text FROM tweets WHERE user_id = ? AND impact > 0 AND " +
              "create_at BETWEEN ? AND ? " +
              "ORDER BY impact DESC, id ASC LIMIT ?"
      );
      q3NegStatement = conn.prepareStatement(
          "SELECT create_at, impact, id, text FROM tweets WHERE user_id = ? AND impact < 0 AND " +
              "create_at BETWEEN ? AND ? " +
              "ORDER BY impact ASC, id ASC LIMIT ?"
      );

      q3PosStatement.setLong(1, Long.valueOf(userId));
      q3PosStatement.setInt(2, skewedStartDate);
      q3PosStatement.setInt(3, skewedEndDate);
      q3PosStatement.setInt(4, n);
      rsPos = q3PosStatement.executeQuery();

      // Parse the result set
      answer.append("Omegaga's Black Railgun,6537-0651-1730\nPositive Tweets\n");
      while (rsPos.next()) {
        long tweetId = rsPos.getLong("ID");
        long timestamp = rsPos.getInt("CREATE_AT");
        Date date = new Date(TweetProcessor.unskewTimeStamp(timestamp));
        int impact = rsPos.getInt("IMPACT");
        String text = rsPos.getString("TEXT");

        answer.append(outputDateFormat.format(date));
        answer.append(",");
        answer.append(impact);
        answer.append(",");
        answer.append(tweetId);
        answer.append(",");
        answer.append(text);
        answer.append("\n");
      }

      q3NegStatement.setLong(1, Long.valueOf(userId));
      q3NegStatement.setInt(2, skewedStartDate);
      q3NegStatement.setInt(3, skewedEndDate);
      q3NegStatement.setInt(4, n);
      rsNeg = q3NegStatement.executeQuery();
      answer.append("\nNegative Tweets\n");
      while (rsNeg.next()) {
        long tweetId = rsNeg.getLong("ID");
        long timestamp = rsNeg.getInt("CREATE_AT");
        Date date = new Date(TweetProcessor.unskewTimeStamp(timestamp));
        int impact = rsNeg.getInt("IMPACT");
        String text = rsNeg.getString("TEXT");

        answer.append(outputDateFormat.format(date));
        answer.append(",");
        answer.append(impact);
        answer.append(",");
        answer.append(tweetId);
        answer.append(",");
        answer.append(text);
        answer.append("\n");
      }



    } catch (SQLException ex) {
      System.out.println("SQLException: " + ex.getMessage());
      System.out.println("SQLState: " + ex.getSQLState());
      System.out.println("VendorError: " + ex.getErrorCode());
    } finally {
      if( rsPos != null ) {
        try {
          rsPos.close();
        } catch (SQLException e) {} // ignore
      }
      if( rsNeg != null ) {
        try {
          rsNeg.close();
        } catch (SQLException e) {} // ignore
      }
      if( q3PosStatement != null ){
        try{
          q3PosStatement.close();
        } catch( SQLException e) {} // ignore
      }
      if( q3NegStatement != null ){
        try{
          q3NegStatement.close();
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
  public static String getSqlAnswerQ4(String hashtag, int n) {
    final DateFormat outputDateFormat =
        new SimpleDateFormat("yyyy-MM-dd");
    outputDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
    Connection conn = null;
    // Check whether the requested timestamp is valid or not
    ResultSet rs = null;
    final StringBuilder answer = new StringBuilder();
    int hashCode = hashtag.hashCode();
    if (hashCode < 0)
      hashCode *= -1;
    hashCode %= SHARDING_COUNT;
    PreparedStatement q4Statement = null;

    // Request a new connection for every request
    try {
      conn = cpds[hashCode].getConnection();

      //System.out.println("Connect to SQL");

      // May cause problem
      conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);

      q4Statement = conn.prepareStatement(
          "SELECT value FROM hashtags WHERE hashtag = ? ORDER BY rank ASC LIMIT ?"
      );

      q4Statement.setString(1, hashtag);
      q4Statement.setInt(2, n);
      rs = q4Statement.executeQuery();

      // Parse the result set
      answer.append("Omegaga's Black Railgun,6537-0651-1730\n");
      while (rs.next()) {
        String value = rs.getString("VALUE");

        answer.append(value);
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
      if( q4Statement != null ){
        try{
          q4Statement.close();
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

  public static String getSqlAnswerQ6(long tweetId){
    return "";
  }

}
