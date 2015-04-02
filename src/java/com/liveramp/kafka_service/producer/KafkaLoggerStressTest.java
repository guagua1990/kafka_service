package com.liveramp.kafka_service.producer;

import java.io.FileNotFoundException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.liveramp.kafka_service.producer.config.YamlProducerConfigBuilder;
import com.liveramp.kafka_service.server.KafkaTopicHelper;
import com.liveramp.kafka_service.zookeeper.ZookeeperClient;
import com.liveramp.kafka_service.zookeeper.ZookeeperClientBuilder;
import com.rapleaf.spruce_lib.log.EntryLogger;

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
      for (AttributionLogGenerator.AttributionLogBuilder log : AttributionLogGenerator.buildNLogs(n)) {
        logger.writeLogEntry(log);
      }
      return null;
    }
  }

  public static void main(String[] args) throws InterruptedException, FileNotFoundException {
    KafkaLogger logger = new KafkaLogger(YamlProducerConfigBuilder.buildFromYaml("config/producer.yaml"), null);

    ExecutorService service = Executors.newFixedThreadPool(8);

    for (int i = 0; i < 10; i++) {
      service.submit(new WriteLogs(logger, i));
    }

    service.shutdown();
    service.awaitTermination(10, TimeUnit.SECONDS);
    logger.close();
  }
}
