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
}
