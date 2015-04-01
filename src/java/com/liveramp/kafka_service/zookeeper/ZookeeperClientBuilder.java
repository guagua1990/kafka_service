package com.liveramp.kafka_service.zookeeper;

public class ZookeeperClientBuilder {
  private String connection;
  private int sessionTimeout;
  private int connectionTimeout;

  private ZookeeperClientBuilder(String connection) {
    this.connection = connection;
    this.sessionTimeout = ZookeeperClient.DEFAULT_SESSION_TIMEOUT_MILLIS;
    this.connectionTimeout = ZookeeperClient.DEFAULT_CONNECTION_TIMEOUT_MILLIS;
  }

  public static ZookeeperClientBuilder from(String connection) {
    return new ZookeeperClientBuilder(connection);
  }

  public ZookeeperClientBuilder setSessionTimeout(int sessionTimeout) {
    this.sessionTimeout = sessionTimeout;
    return this;
  }

  public ZookeeperClientBuilder setConnectionTimeout(int connectionTimeout) {
    this.connectionTimeout = connectionTimeout;
    return this;
  }

  public ZookeeperClient build() {
    return new ZookeeperClient(connection, sessionTimeout, connectionTimeout);
  }
}
