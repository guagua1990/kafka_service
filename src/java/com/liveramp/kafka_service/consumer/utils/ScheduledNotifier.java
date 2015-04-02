package com.liveramp.kafka_service.consumer.utils;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

public class ScheduledNotifier {

  public final long interval;

  private final Timer timer;
  private final AtomicBoolean sendStatsFlag;
  private final static long DELAY = 10 * 1000;

  public ScheduledNotifier(long interval, AtomicBoolean sendStatsFlag) {
    this.interval = interval;
    this.sendStatsFlag = sendStatsFlag;
    timer = new Timer(true);
  }

  public void start() {
    timer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        sendStatsFlag.set(true);
      }
    }, DELAY, interval);
  }

  public void cancel() {
    timer.cancel();
  }

}
