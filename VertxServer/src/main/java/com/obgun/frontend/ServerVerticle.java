package com.obgun.frontend;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.impl.StringEscapeUtils;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Jianhong Li on 10/30/15.
 * Server class
 */
public class ServerVerticle extends AbstractVerticle {
  private static int PORT = 80;

    public ServerVerticle(){
        vertx = io.vertx.core.Vertx.vertx(new VertxOptions().setWorkerPoolSize(200));
    }
    final static ExecutorService threadPool = Executors.newFixedThreadPool(200);
  final private static void handleQ1(RoutingContext routingContext) {
    routingContext.response().end(
        com.obgun.frontend.Q1Util.retStringGenerator(routingContext.request().getParam("key"),
                routingContext.request().getParam("message")));
  }
    final private static void handleHeartBeat(RoutingContext routingContext) {
        routingContext.response().end("xx CC");
    }


  final private void handleQ2HBase(RoutingContext routingContext) {
      vertx.<String>executeBlocking(future -> {

          // Do the blocking operation in here

          // Imagine this was a call to a blocking API to get the result
          String userid = routingContext.request().getParam("userid");
          String timeStamp = routingContext.request().getParam("tweet_time");
          String result = "Omegaga's Black Railgun,6537-0651-1730\n";
          try {

              result += HbaseHandler.getHbaseAnswerQ2(userid, timeStamp);
          } catch (Exception ignore) {
              System.out.println("Q2 bad request: " + userid + " " + timeStamp);
          }
          future.complete(result);

      }, false, res -> {
          if (res.succeeded()) {
              routingContext.response().putHeader("content-type", "text/plain").end(res.result());
          } else {
              res.cause().printStackTrace();
          }
      });
  }
    final private void handleQ3HBase(RoutingContext routingContext) {
        vertx.<String>executeBlocking(future -> {

            // Do the blocking operation in here

            // Imagine this was a call to a blocking API to get the result
            String result = "Omegaga's Black Railgun,6537-0651-1730\n";
            try {
                result += HbaseHandler.getHbaseAnswerQ3(routingContext.request().getParam("userid"),
                        routingContext.request().getParam("start_date"),
                        routingContext.request().getParam("end_date"),
                        routingContext.request().getParam("n"));
            } catch (Exception ignore) {
                System.out.println("Q3 bad request: " + routingContext.request().getParam("userid") + " " + routingContext.request().getParam("start_date")
                +" "+ routingContext.request().getParam("end_date"));
            }
            future.complete(result);

        }, false, res -> {
            if (res.succeeded()) {
                routingContext.response().putHeader("content-type", "text/plain").end(res.result());
            } else {
                res.cause().printStackTrace();
            }
        });
    }

    final static private void handleQ2ThreadPool(RoutingContext routingContext){
        final class Worker implements Runnable{
            public void run(){
                String userid = routingContext.request().getParam("userid");
                String timeStamp = routingContext.request().getParam("tweet_time");
                String result = "Omegaga's Black Railgun,6537-0651-1730\n" +
                        HbaseHandler.getHbaseAnswerQ2(userid, timeStamp);
                routingContext.response().putHeader("content-type", "text/plain").end(result);
            }
        }

        Runnable worker = new Worker();
        threadPool.execute(worker);
    }


    final private void handleQ4HBase(RoutingContext routingContext){
        vertx.<String>executeBlocking(future -> {

            // Do the blocking operation in here

            // Imagine this was a call to a blocking API to get the result
            String hashtag = routingContext.request().getParam("hashtag");
            String rank = routingContext.request().getParam("n");
            String result = "Omegaga's Black Railgun,6537-0651-1730\n";
            try {
                result += HbaseHandler.getHbaseAnswerQ4(hashtag, rank);
            } catch (Exception ignore) {
                System.out.println("Q4 bad request: " + hashtag + " " + rank);
            }
            future.complete(result);

        }, false, res -> {
            if (res.succeeded()) {
                routingContext.response().putHeader("content-type", "text/plain").end(res.result());
            } else {
                res.cause().printStackTrace();
            }
        });
    }


    final private void handleQ4InMM(RoutingContext routingContext){
        vertx.<String>executeBlocking(future -> {
            // Do the blocking operation in here

            // Imagine this was a call to a blocking API to get the result
            final String hashtag = routingContext.request().getParam("hashtag");
            final String rank = routingContext.request().getParam("n");
            // TODO: ugly http request
            String url = null;
            try {
                url = "http://127.0.0.1/qq?hashtag=" + StringEscapeUtils.escapeJava(hashtag) + "&n=" + rank;
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("Bad hashtag: " + hashtag);
            }
            String result = "Omegaga's Black Railgun,6537-0651-1730\n";
            try {
                result += HttpRequest.sendGet(url);
            } catch (Exception ignore) {
                ignore.printStackTrace();
                System.out.println("Q4 bad request: " + hashtag + " " + rank);
            }
            future.complete(result);
        }, false, res -> {
            if (res.succeeded()) {
                routingContext.response().putHeader("content-type", "text/plain").end(res.result());
            } else {
                res.cause().printStackTrace();
            }
        });
    }

    final static private void handleQQ(RoutingContext routingContext){
        final String hashtag = routingContext.request().getParam("hashtag");
        final String rank = routingContext.request().getParam("n");
        String result = "";
        try {
            result += Q4MemStore.getQ4Response(StringEscapeUtils.unescapeJava(hashtag), rank);
        } catch (Exception ignore) {
            ignore.printStackTrace();
            System.out.println("QQ bad request: " + hashtag + " " + rank);
        }
        routingContext.response().putHeader("content-type", "text/plain").end(result);
    }

  @Override
  final public void start() throws Exception {
    // JDBCClient client = JDBCClient.createShared(vertx, config);
      System.out.println(context.config());
      final String team = "ec2-54-152-34-253.compute-1.amazonaws.com";
    HbaseHandler.setHbase(team);
    final Router router = Router.router(vertx);
    router.route().handler(BodyHandler.create());
    router.get("/q1").handler(ServerVerticle::handleQ1);
      router.get("/q2").handler(this::handleQ2HBase);
      router.get("/q3").handler(this::handleQ3HBase);
      router.get("/q4").handler(this::handleQ4HBase);
      router.get("/heartbeat").handler(ServerVerticle::handleHeartBeat);
    vertx.createHttpServer()
        .requestHandler(router::accept)
        .listen(PORT);
  }
}
