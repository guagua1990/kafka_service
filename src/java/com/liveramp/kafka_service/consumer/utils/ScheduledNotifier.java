package com.liveramp.kafka_service.consumer.utils;

import java.util.Timer;
import java.util.TimerTask;

import com.rapleaf.support.DayOfYear;

public class ScheduledNotifier {

  public final long interval;

  private final Thread targetThread;
  private final Timer timer;

  public ScheduledNotifier(Thread thread, long interval) {
    this.targetThread = thread;
    this.interval = interval;
    timer = new Timer(true);
  }

  public void start() {
    timer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        targetThread.interrupt();
      }
    }, DayOfYear.today().toMillis(), interval);
  }

  public void cancel() {
    timer.cancel();
  }

}
