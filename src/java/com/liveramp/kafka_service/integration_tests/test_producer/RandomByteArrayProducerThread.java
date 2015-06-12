package com.liveramp.kafka_service.integration_tests.test_producer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.RandomUtils;

import com.liveramp.kafka_service.producer.ByteArrayProducer;
import com.liveramp.kafka_service.producer.config.ProducerConfigBuilder;
import com.liveramp.kafka_service.zookeeper.ZookeeperServers;

public class RandomByteArrayProducerThread extends Thread {

  private final ByteArrayProducer producer;
  private final String topic;
  private final int numRecords;

  private static String BROKERS_URL = "s2s-data-syncer00:9092,s2s-data-syncer01:9092,s2s-data-syncer02:9092,s2s-data-syncer03:9092,s2s-data-syncer04:9092";
  private static String TOPIC = "byte_array-test";
  private static int N_THREADS = 10;
  private static int N_RECORDS = 1000 * 1000;

  public RandomByteArrayProducerThread(ByteArrayProducer producer, String topic, int numRecords) {
    this.producer = producer;
    this.topic = topic;
    this.numRecords = numRecords;
  }

  @Override
  public void run() {
    for (int i = 0; i < numRecords; i++) {
      producer.send(topic, RandomUtils.nextBytes(20));
    }
  }

  public static void main(String[] args) {

    String brokerUrls;
    String topic;
    int numThreads;
    int numRecords;
    if (args.length > 0) {
      Preconditions.checkArgument(args.length == 4, "Usage: brokersUrl topic_name num_threads num_records_per_thread...");
      brokerUrls = args[0];
      topic = args[1];
      numThreads = Integer.parseInt(args[2]);
      numRecords = Integer.parseInt(args[3]);
    } else {
      brokerUrls = BROKERS_URL;
      topic = TOPIC;
      numThreads = N_THREADS;
      numRecords = N_RECORDS;
    }

    ByteArrayProducer producer = new ByteArrayProducer(ProducerConfigBuilder.createConfig("production_brokers", brokerUrls, 10000));
    ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

    for (int i = 0; i < numThreads; i++) {
      RandomByteArrayProducerThread thread = new RandomByteArrayProducerThread(producer, topic, numRecords);
      executorService.submit(thread);
    }

    Runtime.getRuntime().addShutdownHook(new ZookeeperServers.ShutdownHook(Sets.newHashSet(executorService)));
  }
}
