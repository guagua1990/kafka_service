package com.liveramp.kafka_service.zookeeper;

import java.util.EnumSet;

import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZookeeperClient {
  private static final Logger LOG = LoggerFactory.getLogger(ZookeeperClient.class);
  public static final int DEFAULT_SESSION_TIMEOUT_MILLIS = 10000;
  public static final int DEFAULT_CONNECTION_TIMEOUT_MILLIS = 10000;

  private final ZkClient zkClient;

  private ZookeeperClient(String connections, int sessionTimeout, int connectionTimeout) {
    this.zkClient = new ZkClient(connections, sessionTimeout, connectionTimeout, ZKStringSerializer$.MODULE$);
  }

  public ZkClient get() {
    return zkClient;
  }

  public void close() {
    zkClient.close();
  }

  public boolean createNode(String node) {
    if (!zkClient.exists(node)) {
      zkClient.createPersistent(node, true);
      return true;
    }

    return false;
  }

  public boolean deleteNode(String node, boolean recursive) {
    if (zkClient.exists(node)) {
      if (recursive) {
        return zkClient.deleteRecursive(node);
      } else {
        return zkClient.delete(node);
      }
    }

    return false;
  }

  public static class Builder {
    private final String connections;
    private int sessionTimeout = DEFAULT_SESSION_TIMEOUT_MILLIS;
    private int connectionTimeout = DEFAULT_CONNECTION_TIMEOUT_MILLIS;

    public Builder(EnumSet<ZKEnv.ZKEnsembles> zkEnsembleses) {
      this(ZKEnv.getZkClientConnections(zkEnsembleses));
    }

    public Builder(String connections) {
      this.connections = connections;
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
      return new ZookeeperClient(connections, sessionTimeout, connectionTimeout);
    }
  }

  public static void main(String[] args) {
    ZookeeperClient client = new Builder(ZKEnv.getZKInstances()).build();
    client.deleteNode("/consumers/new/1", true);
    client.createNode("/consumers/new/1/3");
    System.out.println(ZkFs.prettyPrintTree(ZkFs.readingCurrentFs(client.get(), new ZkFs.Directory("/"))));
    client.deleteNode("/consumers/new/1/3", true);
    client.createNode("/consumers/new/1/4");
    System.out.println(ZkFs.prettyPrintTree(ZkFs.readingCurrentFs(client.get(), new ZkFs.Directory("/"))));
    client.close();
  }
}
