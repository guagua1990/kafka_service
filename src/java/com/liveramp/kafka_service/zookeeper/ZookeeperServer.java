package com.liveramp.kafka_service.zookeeper;

import java.util.Properties;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperServer {
  private static final Logger LOG = LoggerFactory.getLogger(ZookeeperServer.class);

  private Properties properties;

  ZookeeperServer(Properties properties) {
    this.properties = properties;
  }

  public void start() throws Exception {
    final QuorumPeerConfig quorumPeerConfig = new QuorumPeerConfig();
    quorumPeerConfig.parseProperties(properties);

    final QuorumPeerMain quorumPeerMain = new QuorumPeerMain();
    quorumPeerMain.runFromConfig(quorumPeerConfig);
  }

  public static void main(String[] args) throws Exception {
    final ZookeeperServer server = ZookeeperServerBuilder.create()
        .setClientPort(2181)
        .setInitLimit(10)
        .setSyncLimit(5)
        .setDataDir("/tmp/zookeeper/data")
        .setDataLogDir("/tmp/zookeeper/log")
        .addServer("1", "10.99.32.1:2888:3888")
        .addServer("2", "10.99.32.14:2888:3888")
        .addServer("3", "10.99.32.36:2888:3888")
        .build();

    Thread thread = new Thread() {
      @Override
      public void run() {
        try {
          server.start();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    };
    thread.run();
  }
}
