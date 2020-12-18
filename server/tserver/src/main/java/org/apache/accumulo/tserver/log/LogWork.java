package org.apache.accumulo.tserver.log;

import org.apache.accumulo.core.client.Durability;

import java.util.concurrent.CountDownLatch;

class LogWork {
  final CountDownLatch latch;
  final Durability durability;
  volatile Exception exception;

  public LogWork(CountDownLatch latch, Durability durability) {
    this.latch = latch;
    this.durability = durability;
  }
}
