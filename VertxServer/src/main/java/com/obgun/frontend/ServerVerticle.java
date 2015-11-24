package com.obgun.frontend;

import io.vertx.core.AbstractVerticle;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import org.jruby.RubyProcess;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Jianhong Li on 10/30/15.
 * Server class
 */
public class ServerVerticle extends AbstractVerticle {
    private static int PORT = 80;
    private static final String[] SERVERS = {
            "ec2-54-152-34-253.compute-1.amazonaws.com",
            "ec2-54-174-128-58.compute-1.amazonaws.com",
            "ec2-54-172-51-52.compute-1.amazonaws.com",
            "ec2-54-164-126-14.compute-1.amazonaws.com",
            "ec2-52-91-220-101.compute-1.amazonaws.com",
            "ec2-54-88-191-62.compute-1.amazonaws.com"
    };


  final private static void handleQ1(RoutingContext routingContext) {
    routingContext.response().end(
        com.obgun.frontend.Q1Util.retStringGenerator(routingContext.request().getParam("key"),
                routingContext.request().getParam("message")));
  }
    final private static void handleHeartBeat(RoutingContext routingContext) {
        routingContext.response().end("xx CC");
    }

  final private void handleQ2MySQL(RoutingContext routingContext) {
      vertx.<String>executeBlocking(future -> {

        // Do the blocking operation in here

        // Imagine this was a call to a blocking API to get the result
        String userid = routingContext.request().getParam("userid");
        String timeStamp = routingContext.request().getParam("tweet_time");
        String result = "";
        try {
          result = SQLHandler.getSqlAnswerQ2(userid, timeStamp);
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

  final private void handleQ3MySQL(RoutingContext routingContext) {
    vertx.<String>executeBlocking(future -> {

      // Do the blocking operation in here

      // Imagine this was a call to a blocking API to get the result
      final String userid = routingContext.request().getParam("userid");
      final String startTs = routingContext.request().getParam("start_date");
      final String endTs = routingContext.request().getParam("end_date");
      final int n = Integer.valueOf(routingContext.request().getParam("n"));
      String result = "";
      try {
        result = SQLHandler.getSqlAnswerQ3(userid, startTs, endTs, n);
      } catch (Exception ignore) {
        System.out.println("Q3 bad request: " + userid + " " + startTs);
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



  final private void handleQ4MySQL(RoutingContext routingContext) {
    vertx.<String>executeBlocking(future -> {

      // Do the blocking operation in here

      // Imagine this was a call to a blocking API to get the result
      final String hashtag = routingContext.request().getParam("hashtag");
      final int n = Integer.valueOf(routingContext.request().getParam("n"));
      String result = "";
      try {
        result = SQLHandler.getSqlAnswerQ4(hashtag, n);
      } catch (Exception ignore) {
        System.out.println("Q4 bad request: " + hashtag + " " + n);
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

  final private void handleQ5MySQL(RoutingContext routingContext) {
    vertx.<String>executeBlocking(future -> {

      // Do the blocking operation in here

      // Imagine this was a call to a blocking API to get the result
      final long startUid = Long.valueOf(routingContext.request().getParam("userid_min"));
      final long endUid = Long.valueOf(routingContext.request().getParam("userid_max"));
      String result = "";
      try {
        result = SQLHandler.getSqlAnswerQ5(startUid, endUid);
      } catch (Exception ignore) {
        System.out.println("Q5 bad request: " + startUid + " " + endUid);
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

    final ConcurrentHashMap<Long, TransactionUnit> q6Map = new ConcurrentHashMap<>();
    final private void handleQ6(RoutingContext routingContext){
        //q6?tid=3000001&opt=s
        //q6?tid=3000001&seq=1&opt=a&tweetid=458875845231521792&tag=ILOVE15619!12
        //q6?tid=3000001&seq=5&opt=r&tweetid=448988310417850370
        //q6?tid=3000001&opt=e
        final String result = "Omegaga's Black Railgun,6537-0651-1730\n";
        long tid = Long.parseLong(routingContext.request().getParam("tid"));
        char opt = routingContext.request().getParam("opt").charAt(0);
        switch (opt){
            case 's': {
                routingContext.response().putHeader("content-type", "text/plain").end(result + "0\n");
                vertx.<Integer>executeBlocking(future -> {
                    synchronized (q6Map){
                        if(q6Map.get(tid) == null){
                            q6Map.put(tid, new TransactionUnit(tid));
                        }
                    }
                    future.complete(1);
                }, false, res -> {
                    if (res.succeeded()) {} else {
                        res.cause().printStackTrace();
                    }
                }); break;

            }
            case 'a': {
                String tag = routingContext.request().getParam("tag");
                String sId = routingContext.request().getParam("seq");
                String tweetId = routingContext.request().getParam("tweetid");
                routingContext.response().putHeader("content-type", "text/plain").end(result + tag + "\n");
                vertx.<Integer>executeBlocking(future -> {
                    synchronized (q6Map){
                        if(q6Map.get(tid) == null){
                            q6Map.put(tid, new TransactionUnit(tid));
                        }
                    }
                    q6Map.get(tid).write(sId, tweetId, tag);
                    future.complete(1);
                }, false, res -> {
                    if (res.succeeded()) {} else {
                        res.cause().printStackTrace();
                    }
                }); break;
            }
            case 'r':
                vertx.<String>executeBlocking(future -> {
                    String sId = routingContext.request().getParam("seq");
                    String tweetId = routingContext.request().getParam("tweetid");
                    synchronized (q6Map){
                        if(q6Map.get(tid) == null){
                            q6Map.put(tid, new TransactionUnit(tid));
                        }
                    }
                    String text = q6Map.get(tid).read(sId, tweetId);
                    future.complete(result+text+"\n");
                }, false, res -> {
                    if (res.succeeded()) {
                        routingContext.response().putHeader("content-type", "text/plain").end(res.result());
                    } else {
                        res.cause().printStackTrace();
                    }
                }); break;
            case 'e':
                routingContext.response().putHeader("content-type", "text/plain").end(result+"0\n");
                vertx.<String>executeBlocking(future -> {
                    synchronized (q6Map){
                        if(q6Map.get(tid) == null){
                            q6Map.put(tid, new TransactionUnit(tid));
                        }
                    }
                    q6Map.get(tid).endChecker();
                    future.complete(result + "0");
                }, false, res -> {
                    if (res.succeeded()) {
                        q6Map.remove(tid);
                    } else {
                        res.cause().printStackTrace();
                    }
                }); break;
            default: System.out.println(routingContext.request());break;
        }
    }

  @Override
  final public void start() throws Exception {
    // JDBCClient client = JDBCClient.createShared(vertx, config);
    String[] urls = {
        "ec2-52-91-99-140.compute-1.amazonaws.com",
        "ec2-54-173-93-194.compute-1.amazonaws.com",
        "ec2-52-91-183-75.compute-1.amazonaws.com",
        "ec2-54-173-94-235.compute-1.amazonaws.com",
        "ec2-52-23-156-246.compute-1.amazonaws.com",
        "ec2-54-172-98-131.compute-1.amazonaws.com",
        "ec2-52-91-170-163.compute-1.amazonaws.com",
        "ec2-54-152-144-114.compute-1.amazonaws.com"
    };
    SQLHandler.setMySql(urls, "obgun", "D807isfuckingyou");
    //HbaseHandler.setHbase("ec2-54-152-174-216.compute-1.amazonaws.com");
    final Router router = Router.router(vertx);
    router.route().handler(BodyHandler.create());
    router.get("/q1").handler(ServerVerticle::handleQ1);
    //router.get("/q2").handler(ServerVerticle::handleQ2ThreadPool);
    //router.get("/q2").handler(this::handleQ2);
    router.get("/q2").handler(this::handleQ2MySQL);
    router.get("/q3").handler(this::handleQ3MySQL);
    router.get("/q4").handler(this::handleQ4MySQL);
    router.get("/q5").handler(this::handleQ5MySQL);
      router.get("/q6").handler(this::handleQ6);
    router.get("/heartbeat").handler(ServerVerticle::handleHeartBeat);
    vertx.createHttpServer()
        .requestHandler(router::accept)
        .listen(PORT);
  }
}
