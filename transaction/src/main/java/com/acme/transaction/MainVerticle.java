package com.acme.transaction;

import com.acme.transaction.impl.TransactionServiceImpl;

import io.vertx.config.ConfigRetriever;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.serviceproxy.ServiceBinder;

public class MainVerticle extends AbstractVerticle {

  private TransactionServiceImpl service;

  @Override
  public void start(Future<Void> future) throws Exception {
    ConfigRetriever.create(vertx).getConfig(getConfig -> {
      if (getConfig.failed()) {
        future.fail(getConfig.cause());
      } else {
        service = new TransactionServiceImpl(vertx, config());
        new ServiceBinder(vertx)
          .setAddress(TransactionService.DEFAULT_ADDRESS)
          .register(TransactionService.class, service);
        future.complete();
      }
    });
  }

  @Override
  public void stop() throws Exception {
    service.close();
  }
}
