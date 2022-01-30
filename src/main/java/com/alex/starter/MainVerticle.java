package com.alex.starter;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.jdbcclient.JDBCConnectOptions;
import io.vertx.jdbcclient.JDBCPool;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.eventbus.EventBus;
import io.vertx.reactivex.ext.web.Router;

public class MainVerticle extends AbstractVerticle {
  public static void main(String[] args) {
    Vertx vertx= Vertx.vertx();
    vertx.deployVerticle(MainVerticle.class.getName());
  }

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    EventBus bus= vertx.eventBus();
    Router router = Router.router(vertx);
    JsonObject jdbcconfig = new JsonObject()
      .put("maxPoolSize", 30)
      .put("user", "root")
      .put("password", "root")
      .put("url", "jdbc:mysql://localhost:3306/testDb?allowPublicKeyRetrieval=true&&useSSL=false&&generateSimpleParameterMetadata=true")
      .put("driver_class", "com.mysql.cj.jdbc.Driver")
      .put("max_idle_time", 5);

    JsonObject config = new JsonObject()
      .put("jdbcconfig", jdbcconfig)
      //.put("application.clientUrl", CLIENT_URL)
      .put("http.cors.allowed.origin", "*");

    DeploymentOptions options= new DeploymentOptions()
      .setConfig(config)
      .setWorkerPoolSize(10)
      .setWorker(true)
      .setHa(true);

    vertx.deployVerticle(UserDBVerticle.class.getName(),options);

    vertx.createHttpServer()
      .requestHandler(request-> {
        request.bodyHandler(handler-> {
          JsonObject payload= handler.toJsonObject();
          if(payload.getLong("code")==1001){
            bus.publish("addUser",payload);
          }
        });
      })
      .listen(9002, http -> {
      if (http.succeeded()) {
        startPromise.complete();
        System.out.println("HTTP server started on port 9002");
      } else {
        startPromise.fail(http.cause());
      }
    });
  }
}
