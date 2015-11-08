package com.obgun.frontend;

import io.vertx.core.AbstractVerticle;
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
    ExecutorService threadPool = Executors.newFixedThreadPool(200);
  private static void handleQ1(RoutingContext routingContext) {
    routingContext.response().end(
        com.obgun.frontend.Q1Util.retStringGenerator(routingContext.request().getParam("key"),
                routingContext.request().getParam("message")));
  }

  private void handleQ2(RoutingContext routingContext) {
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

    private void handleQ2ThreadPool(RoutingContext routingContext){
        class Worker implements Runnable{
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

  @Override
  public void start() throws Exception {
    // JDBCClient client = JDBCClient.createShared(vertx, config);
    HbaseHandler.setHbase("ec2-54-172-51-57.compute-1.amazonaws.com");
    final Router router = Router.router(vertx);
    router.route().handler(BodyHandler.create());
    router.get("/q1").handler(ServerVerticle::handleQ1);
    router.get("/q2").handler(this::handleQ2ThreadPool);
    vertx.createHttpServer()
        .requestHandler(router::accept)
        .listen(PORT);
  }
}
