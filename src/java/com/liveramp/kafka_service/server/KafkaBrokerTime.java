package com.liveramp.kafka_service.server;

import kafka.utils.Time;

public class KafkaBrokerTime implements Time {

  public long milliseconds() {
    return System.currentTimeMillis();
  }

  public long nanoseconds() {
    return System.nanoTime();
  }

  public void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      // Ignore
    }
  }

}
