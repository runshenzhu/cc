package com.obgun.frontend;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;

public class Main{

    public static void main(String[] args) throws Exception {
        //System.setProperty("vertx.disableFileCaching", "true");
        //System.out.println("in main");
        if( args.length != 1 ){
            System.out.println("java for <inputQ4file>");
        }

       

        final VertxOptions options = new VertxOptions().setWorkerPoolSize(128);
        final Vertx vertx = Vertx.vertx(options);
        vertx.deployVerticle("com.obgun.frontend.ServerVerticle");
    }


}