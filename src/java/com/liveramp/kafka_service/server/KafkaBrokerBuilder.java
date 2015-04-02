package com.liveramp.kafka_service.server;

import java.util.Properties;

public class KafkaBrokerBuilder {
  private final int brokerId;
  private final Properties properties;

  private KafkaBrokerBuilder(int brokerId) {
    this.brokerId = brokerId;
    this.properties = new Properties();
    setProperty("num.network.threads", 3);
    setProperty("num.io.threads", 8);
    setProperty("socket.send.buffer.bytes", 102400);
    setProperty("socket.receive.buffer.bytes", 65536);
    setProperty("socket.request.max.bytes", 104857600);
    setProperty("log.dirs", "/tmp/kafka-logs");
    setProperty("num.partitions", 1);
    setProperty("num.recovery.threads.per.data.dir", 1);
    setProperty("log.retention.hours", 168);
    setProperty("log.segment.bytes", 1073741824);
    setProperty("log.retention.check.interval.ms", 300000);
    setProperty("log.cleaner.enable", false);
    setProperty("zookeeper.connection.timeout.ms", 2000);
    setProperty("controlled.shutdown.enabl", true);
    setProperty("broker.id", String.valueOf(brokerId));
  }

  public static KafkaBrokerBuilder create(int brokerId) {
    return new KafkaBrokerBuilder(brokerId);
  }

  public KafkaBrokerBuilder setPort(int port) {
    return setProperty("port", String.valueOf(port));
  }

  public KafkaBrokerBuilder setLogDirs(String logDirs) {
    return setProperty("log.dirs", logDirs);
  }

  public KafkaBrokerBuilder setDeleteTopicEnable(boolean deleteTopicEnable) {
    return setProperty("delete.topic.enable", deleteTopicEnable);
  }

  public KafkaBrokerBuilder setZookeeperConnect(String zkConnect) {
    return setProperty("zookeeper.connect", zkConnect);
  }

  private KafkaBrokerBuilder setProperty(String key, String value) {
    this.properties.setProperty(key, value);
    return this;
  }

  private KafkaBrokerBuilder setProperty(String key, int value) {
    this.properties.setProperty(key, String.valueOf(value));
    return this;
  }

  private KafkaBrokerBuilder setProperty(String key, boolean value) {
    this.properties.setProperty(key, String.valueOf(value));
    return this;
  }

  public KafkaBroker build() {
    return new KafkaBroker(brokerId, properties, new KafkaBrokerTime());
  }
}
