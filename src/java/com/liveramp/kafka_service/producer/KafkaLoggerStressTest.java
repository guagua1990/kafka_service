package com.liveramp.kafka_service.producer;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Joiner;
import kafka.producer.ProducerConfig;

import com.liveramp.kafka_service.producer.config.SyncProducerConfigBuilder;
import com.liveramp.kafka_service.producer.serializer.DefaultStringEncoder;
import com.rapleaf.spruce_lib.log.EntryLogger;
import com.rapleaf.spruce_lib.log.SpruceLogEntry;

public class KafkaLoggerStressTest {

  private static class WriteLogs implements Callable<Void> {

    private final String category;
    private final int index;
    private final EntryLogger logger;

    public WriteLogs(String category, int index, EntryLogger entryLogger) {
      this.category = category;
      this.index = index;
      this.logger = entryLogger;
    }

    @Override
    public Void call() throws Exception {
      for (int i = 0; i < 10; i++) {
        final int id = i;
        logger.writeLogEntry(new SpruceLogEntry(category) {
          @Override
          public String toString() {
            return Joiner.on("-").join(category, "dist" + index, id);
          }
        });
      }
      return null;
    }
  }

  public static void main(String[] args) throws InterruptedException {
    ProducerConfig config = new SyncProducerConfigBuilder(new DefaultStringEncoder())
        .addBroker("localhost", 9092)
        .build();

    String topic = "attribution-test";
    KafkaLogger logger = new KafkaLogger(config);

    ExecutorService service = Executors.newFixedThreadPool(8);

    for (int i = 0; i < 5; i++) {
      service.submit(new WriteLogs(topic, i, logger));
    }

    service.shutdown();
    service.awaitTermination(10, TimeUnit.SECONDS);
    logger.close();
  }
}
