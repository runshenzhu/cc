package com.obgun.frontend;

import java.io.IOException;

/**
 * Created by zrsh on 11/11/15.
 */
import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.HttpURLConnection;

public class HttpRequest {
    static final public String sendGet(String url) {

        int responseCode = 400;
        StringBuffer response = new StringBuffer();
        try {


            URL obj = new URL(url);
            HttpURLConnection con = (HttpURLConnection) obj.openConnection();

            // optional default is GET
            con.setRequestMethod("GET");
            responseCode = con.getResponseCode();
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getInputStream()));
            String inputLine;


            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();
        }catch (IOException e){
            System.out.println("Send url fail: " + e.toString());
            System.out.println(response);
        }finally {
            return response.toString();
        }
    }
}
