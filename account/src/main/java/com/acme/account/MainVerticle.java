package com.acme.account;

import com.acme.account.impl.AccountServiceImpl;

import io.vertx.config.ConfigRetriever;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.serviceproxy.ServiceBinder;

public class MainVerticle extends AbstractVerticle {

  private AccountServiceImpl service;

  @Override
  public void start(Future<Void> future) throws Exception {
    ConfigRetriever retriever = ConfigRetriever.create(vertx);
    retriever.getConfig(getConfig -> {
      if (getConfig.failed()) {
        future.fail(getConfig.cause());
      } else {
        service = new AccountServiceImpl(vertx, config());
        new ServiceBinder(vertx)
          .setAddress(AccountService.DEFAULT_ADDRESS)
          .register(AccountService.class, service);

        future.complete();
      }
    });
  }

  @Override
  public void stop() throws Exception {
    service.close();
  }
}
