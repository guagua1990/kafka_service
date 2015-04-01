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
      for (AttributionLogGenerator.AttributionLogBuilder log : AttributionLogGenerator.buildNLogs(n)) {
        logger.writeLogEntry(log);
      }
      return null;
    }
  }

  public static void main(String[] args) throws InterruptedException, FileNotFoundException {
    ZookeeperClient zookeeperClient = ZookeeperClientBuilder
        .from("10.99.32.1:2181,10.99.32.14:2181,10.99.32.36:2181")
        .build();

    KafkaTopicHelper helper = KafkaTopicHelper.create(zookeeperClient);
    if (!helper.getTopics().contains(AttributionLogGenerator.GOOD_REQUEST_CATEGORY)) {
      helper.createTopic(AttributionLogGenerator.GOOD_REQUEST_CATEGORY, 2, 2);
    }
    System.out.println(helper.getTopics());

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
