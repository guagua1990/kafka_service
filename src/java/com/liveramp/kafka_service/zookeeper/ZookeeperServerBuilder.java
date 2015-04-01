package com.liveramp.kafka_service.zookeeper;

import java.util.Properties;

public class ZookeeperServerBuilder {
  private final Properties properties;

  private ZookeeperServerBuilder() {
    this.properties = new Properties();
  }

  public static ZookeeperServerBuilder create() {
    return new ZookeeperServerBuilder();
  }

  public ZookeeperServerBuilder setDataDir(String dataDir) {
    return setProperty("dataDir", dataDir);
  }

  public ZookeeperServerBuilder setClientPort(int clientPort) {
    return setProperty("clientPort", String.valueOf(clientPort));
  }

  public ZookeeperServerBuilder setDataLogDir(String dataLogDir) {
    return setProperty("dataLogDir", dataLogDir);
  }

  public ZookeeperServerBuilder setInitLimit(int initLimit) {
    return setProperty("initLimit", String.valueOf(initLimit));
  }

  public ZookeeperServerBuilder setSyncLimit(int syncLimit) {
    return setProperty("syncLimit", String.valueOf(syncLimit));
  }

  public ZookeeperServerBuilder setServer(String name, String address) {
    return setProperty("server." + name, address);
  }

  private ZookeeperServerBuilder setProperty(String key, String value) {
    this.properties.setProperty(key, value);
    return this;
  }

  public ZookeeperServer build() {
    return new ZookeeperServer(properties);
  }
}
