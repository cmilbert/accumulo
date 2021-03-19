package org.apache.accumulo.tserver.log;

import java.util.concurrent.CountDownLatch;

import org.apache.accumulo.core.client.Durability;

class LogWork {
  final CountDownLatch latch;
  final Durability durability;
  volatile Exception exception;

  public LogWork(CountDownLatch latch, Durability durability) {
    this.latch = latch;
    this.durability = durability;
  }
}
