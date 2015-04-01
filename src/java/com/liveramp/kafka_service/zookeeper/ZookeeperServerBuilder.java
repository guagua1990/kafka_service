package com.liveramp.kafka_service.zookeeper;

import java.util.Properties;

public class ZookeeperServerBuilder {
  private final Properties properties;

  private ZookeeperServerBuilder() {
    this.properties = new Properties();
    setProperty("broker.id", 0);
    setProperty("port", 9092);
  }

  public static ZookeeperServerBuilder create() {
    return new ZookeeperServerBuilder();
  }

  public ZookeeperServerBuilder setDataDir(String dataDir) {
    return setProperty("dataDir", dataDir);
  }

  public ZookeeperServerBuilder setClientPort(int clientPort) {
    return setProperty("clientPort", clientPort);
  }

  public ZookeeperServerBuilder setDataLogDir(String dataLogDir) {
    return setProperty("dataLogDir", dataLogDir);
  }

  public ZookeeperServerBuilder setInitLimit(int initLimit) {
    return setProperty("initLimit", initLimit);
  }

  public ZookeeperServerBuilder setSyncLimit(int syncLimit) {
    return setProperty("syncLimit", syncLimit);
  }

  public ZookeeperServerBuilder addServer(String name, String address) {
    return setProperty("server." + name, address);
  }

  private ZookeeperServerBuilder setProperty(String key, String value) {
    this.properties.setProperty(key, value);
    return this;
  }

  private ZookeeperServerBuilder setProperty(String key, int value) {
    this.properties.setProperty(key, String.valueOf(value));
    return this;
  }

  private ZookeeperServerBuilder setProperty(String key, boolean value) {
    this.properties.setProperty(key, String.valueOf(value));
    return this;
  }

  public ZookeeperServer build() {
    return new ZookeeperServer(properties);
  }
}
