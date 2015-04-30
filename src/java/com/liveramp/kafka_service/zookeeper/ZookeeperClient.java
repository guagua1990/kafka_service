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

  public static void main(String[] args) throws Exception {

  }
}
