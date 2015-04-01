package com.liveramp.kafka_service.zookeeper;

import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperClient {
  private static final Logger LOG = LoggerFactory.getLogger(ZookeeperClient.class);
  public static final int DEFAULT_SESSION_TIMEOUT_MILLIS = 10000;
  public static final int DEFAULT_CONNECTION_TIMEOUT_MILLIS = 10000;

  private String connection;
  private int sessionTimeout;
  private int connectionTimeout;
  private ZkClient zkClient;

  public ZookeeperClient(String connection, int sessionTimeout, int connectionTimeout) {
    this.connection = connection;
    this.sessionTimeout = sessionTimeout;
    this.connectionTimeout = connectionTimeout;
    this.zkClient = new ZkClient(connection, sessionTimeout, connectionTimeout, ZKStringSerializer$.MODULE$);
  }

  public ZkClient get() {
    return zkClient;
  }

  public static class Builder {
    private String connection;
    private int sessionTimeout;
    private int connectionTimeout;

    private Builder(String connection) {
      this.connection = connection;
      this.sessionTimeout = DEFAULT_SESSION_TIMEOUT_MILLIS;
      this.connectionTimeout = DEFAULT_CONNECTION_TIMEOUT_MILLIS;
    }

    public static Builder from(String connection) {
      return new Builder(connection);
    }

    public Builder setSessionTimeout(int sessionTimeout) {
      this.sessionTimeout = sessionTimeout;
      return this;
    }

    public Builder setConnectionTimeout(int connectionTimeout) {
      this.connectionTimeout = connectionTimeout;
      return this;
    }

    public ZookeeperClient build() {
      return new ZookeeperClient(connection, sessionTimeout, connectionTimeout);
    }
  }

  public static void main(String[] args) {
    ZookeeperClient zookeeperClient = Builder
        .from("10.99.32.1:2181,10.99.32.14:2181,10.99.32.36:2181")
        .build();
  }
}
