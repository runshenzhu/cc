package com.obgun.frontend;

import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Created by Jianhong Li on 10/30/15.
 * Utility functions for Q1
 */
public class Q1Util {

  static final String team_info = "Omegaga's Black Railgun,6537-0651-1730\n";
  static final BigInteger X = new BigInteger("8271997208960872478735181815578166723519929177896558845922250595511921395049126920528021164569045773");

  static void decode(String msg, int shift, StringBuilder sb) {
    final int k = (int)Math.sqrt(msg.length());

    int j, x, sum;
    for (sum = 0; sum < k; sum++) {
      j = sum;
      for (x = 0; x <= sum; x++) {
        // add character from msg to ret, transform according to Caesar decrypt
        sb.append((char) ('A' + ((msg.charAt(j) - 'A' + 26 - shift) % 26)));
        j += k - 1;
      }
    }

    int init = (k << 1) - 1;
    for (sum = k - 2; sum >= 0; sum--){
      j = init;
      for (x = 0; x <= sum; x++) {
        // add character from msg to ret, transform according to Caesar decrypt
        sb.append((char)('A' + ((msg.charAt(j) - 'A' + 26 - shift) % 26)));
        j += k - 1;
      }
      init += k;
    }
  }

  public static String retStringGenerator(String key, String msg) {
    final BigInteger keyInt = new BigInteger(key);
    final BigInteger y = keyInt.divide(X);

    final int z = y.mod(new BigInteger("25")).intValue() + 1;
    final Calendar cal = Calendar.getInstance();
    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss\n");
    final StringBuilder sb = new StringBuilder();
    sb.append(team_info)
      .append(sdf.format(cal.getTime()));
    decode(msg, z, sb);
    sb.append("\n");
    return sb.toString();
  }
}
