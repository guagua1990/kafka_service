package com.liveramp.kafka_service.server;

import java.util.Properties;

public class KafkaBrokerBuilder {
  private final Properties properties;

  private KafkaBrokerBuilder() {
    this.properties = new Properties();
  }

  public static KafkaBrokerBuilder create() {
    return new KafkaBrokerBuilder();
  }

  public KafkaBrokerBuilder setBrokerId(int brokerId) {
    return setProperty("broker.id", String.valueOf(brokerId));
  }

  public KafkaBrokerBuilder setPort(int port) {
    return setProperty("port", String.valueOf(port));
  }

  public KafkaBrokerBuilder setZookeeperConnect(String zkConnect) {
    return setProperty("zookeeper.connect", zkConnect);
  }

  private KafkaBrokerBuilder setProperty(String key, String value) {
    this.properties.setProperty(key, value);
    return this;
  }

  public KafkaBroker build() {
    return new KafkaBroker(properties, new KafkaBrokerTime());
  }
}
