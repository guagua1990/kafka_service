package com.liveramp.kafka_service.zookeeper;

import java.io.IOException;
import java.util.Properties;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperServer {
  private static final Logger LOG = LoggerFactory.getLogger(ZookeeperServer.class);

  private Properties properties;

  private ZookeeperServer(Properties properties) {
    this.properties = properties;
  }

  public void start() throws Exception {
    final QuorumPeerConfig quorumPeerConfig = new QuorumPeerConfig();
    quorumPeerConfig.parseProperties(properties);

    final ServerConfig serverConfig = new ServerConfig();
    serverConfig.readFrom(quorumPeerConfig);

    final ZooKeeperServerMain zooKeeperServer = new ZooKeeperServerMain();
    Thread thread = new Thread() {
      @Override
      public void run() {
        try {
          zooKeeperServer.runFromConfig(serverConfig);
        } catch (IOException e) {
          LOG.error("ZooKeeper Failed", e);
        }
      }
    };
    thread.start();
  }

  public static class Builder {

    private final Properties properties;

    private Builder() {
      this.properties = new Properties();
    }

    public static Builder create() {
      return new Builder();
    }

    public Builder setDataDir(String dataDir) {
      return setProperty("dataDir", dataDir);
    }

    public Builder setClientPort(int clientPort) {
      return setProperty("clientPort", String.valueOf(clientPort));
    }

    public Builder setDataLogDir(String dataLogDir) {
      return setProperty("dataLogDir", dataLogDir);
    }

    public Builder setInitLimit(int initLimit) {
      return setProperty("initLimit", String.valueOf(initLimit));
    }

    public Builder setSyncLimit(int syncLimit) {
      return setProperty("syncLimit", String.valueOf(syncLimit));
    }

    public Builder setServer(String name, String address) {
      return setProperty("server." + name, address);
    }

    private Builder setProperty(String key, String value) {
      this.properties.setProperty(key, value);
      return this;
    }

    public ZookeeperServer build() {
      return new ZookeeperServer(properties);
    }
  }

  public static void main(String[] args) throws Exception {
    ZookeeperServer server = ZookeeperServer.Builder.create()
        .setClientPort(2181)
        .setInitLimit(10)
        .setSyncLimit(5)
        .setDataDir("/tmp/zookeeper/data")
        .setDataLogDir("/tmp/zookeeper/log")
        .setServer("1", "10.99.32.1:2888:3888")
        .setServer("2", "10.99.32.14:2888:3888")
        .setServer("3", "10.99.32.36:2888:3888")
        .build();

    server.start();
  }
}
