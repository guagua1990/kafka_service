package com.liveramp.kafka_service.server;

import java.util.Properties;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Time;

public class KafkaBroker {

  private final Properties properties;
  private final Time time;

  KafkaBroker(Properties properties, Time time) {
    this.properties = properties;
    this.time = time;
  }

  public void start() {
    KafkaConfig config = new KafkaConfig(properties);
    KafkaServer server = new KafkaServer(config, time);
    server.startup();
  }

  public static void main(String[] args) {
    final KafkaBroker broker = KafkaBrokerBuilder.create()
        .setZookeeperConnect("10.99.32.1:2181,10.99.32.14:2181,10.99.32.36:2181")
        .setPort(9092)
        .setDeleteTopicEnable(true)
        .setLogDirs("/tmp/kafka-logs")
        .build();

    Thread thread = new Thread() {
      @Override
      public void run() {
        broker.start();
      }
    };
    thread.run();
  }
}
