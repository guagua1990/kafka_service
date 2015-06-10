package com.liveramp.kafka_service.producer;

import java.util.concurrent.Callable;

import com.rapleaf.spruce_lib.log.EntryLogger;
import com.rapleaf.spruce_lib.log.SpruceLogEntry;

public class KafkaLoggerStressTest {

  private static class WriteLogs implements Callable<Void> {

    private final EntryLogger logger;
    private final int n;

    public WriteLogs(EntryLogger entryLogger, int n) {
      this.logger = entryLogger;
      this.n = n;
    }

    @Override
    public Void call() throws Exception {
      System.out.println("writing " + n + " logs...");
      for (final String log : AttributionLogGenerator.buildNLogs(n)) {
        logger.writeLogEntry(new SpruceLogEntry("total_requests_per_chunk") {
          @Override
          public String toString() {
            return log;
          }
        });
      }
      return null;
    }
  }

}
