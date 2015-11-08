package com.obgun.frontend;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

/**
 * Created by Jianhong Li on 10/30/15.
 * Server class
 */
public class ServerVerticle extends AbstractVerticle {
  private static int PORT = 80;

  private static void handleQ1(RoutingContext routingContext) {
    routingContext.response().end(
        Q1Util.retStringGenerator(routingContext.request().getParam("key"),
            routingContext.request().getParam("message")));
  }

  private void handleQ2(RoutingContext routingContext) {

  }

  @Override
  public void start() throws Exception {
    // JDBCClient client = JDBCClient.createShared(vertx, config);

    final Router router = Router.router(vertx);
    router.route().handler(BodyHandler.create());
    router.get("/q1").handler(ServerVerticle::handleQ1);
    // router.get("/q2").handler(this::handleQ2);
    vertx.createHttpServer()
        .requestHandler(router::accept)
        .listen(PORT);
  }
}
