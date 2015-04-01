package com.liveramp.kafka_service.consumer;

import java.util.Timer;
import java.util.TimerTask;

import com.rapleaf.support.DayOfYear;

public class ScheduledNotifier {

  public static final long INTERVAL = 50 * 1000;

  private final Thread targetThread;
  private final Timer timer;

  public ScheduledNotifier(Thread thread) {
    this.targetThread = thread;
    timer = new Timer(true);
  }

  public void start() {
    timer.scheduleAtFixedRate(new TimerTask() {
      @Override
      public void run() {
        targetThread.interrupt();
      }
    }, DayOfYear.today().toMillis(), INTERVAL);
  }

  public void cancel() {
    timer.cancel();
  }

}
