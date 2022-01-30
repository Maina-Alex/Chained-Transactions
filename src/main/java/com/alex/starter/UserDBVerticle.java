package com.alex.starter;

import at.favre.lib.crypto.bcrypt.BCrypt;
import io.reactivex.Single;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.ResultSet;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.eventbus.EventBus;
import io.vertx.reactivex.core.eventbus.Message;
import io.vertx.reactivex.ext.jdbc.JDBCClient;
import io.vertx.reactivex.ext.sql.SQLConnection;
import io.vertx.reactivex.sqlclient.SqlClient;
import io.vertx.reactivex.sqlclient.SqlConnection;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Alex Maina
 * @created 30/01/2022
 */
public class UserDBVerticle extends AbstractVerticle {
  private JDBCClient sqlClient;
  private final Logger logger= Logger.getLogger(this.getClass().getName());
  @Override
  public void start() throws Exception {
    logger.log(Level.INFO,"Request reached verticle");
    sqlClient= JDBCClient.createShared(vertx, config().getJsonObject("jdbcconfig"), "mysqldatasource");
    EventBus bus= vertx.eventBus();
    bus.consumer("addUser",this::addUser);
  }

  private  void addUser(Message<JsonObject> tMessage) {
    JsonObject requestBody=tMessage.body();
    logger.log(Level.INFO,"Request reached here");
    String firstName= requestBody.getString("firstName");
    String lastName= requestBody.getString("lastName");
    String username= requestBody.getString("username");
    String password= requestBody.getString("password");
    String kinFirstName= requestBody.getJsonObject("kins").getString("firstName");
    String kinLastName= requestBody.getJsonObject("kins").getString("lastName");
   String hashedPassword= BCrypt.withDefaults().hashToString(12, password.toCharArray());
    String addPersonSQL= "insert into person (firstName,lastName,username,password) values (?,?,?,?)";
    JsonArray addUserParam= new JsonArray().add(firstName).add(lastName).add(username).add(hashedPassword);
    String insertKin= "insert into kins(firstName,lastName,person_id) values(?,?,?)";
    JsonArray insertKinParams= new JsonArray().add(kinFirstName).add(kinLastName);
    sqlClient.getConnection(conn->{
      if(conn.succeeded()){
        SQLConnection sqlConnection= conn.result();
        sqlConnection.rxUpdateWithParams(addPersonSQL,addUserParam)
          .flatMap(result-> sqlConnection.rxQueryWithParams("select id from person where username=?", new JsonArray().add(username)))
          .compose(res-> res.map(resultSet -> {
             List<JsonArray> resultList = resultSet.getResults();
             JsonArray jsonArray = resultList.get(0);
             long id = jsonArray.getLong(0);
             System.out.println(id);
             insertKinParams.add(id);
             sqlConnection.rxUpdateWithParams(insertKin, insertKinParams)
               .doOnTerminate(sqlConnection::rxClose)
               .subscribe(result-> System.out.println("updated Kin"));
             return res.toFuture();

           }))
          .subscribe();

      }
    });
  }

}
