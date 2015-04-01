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

  ZookeeperServer(Properties properties) {
    this.properties = properties;
  }

  public void start() throws Exception {
    final QuorumPeerConfig quorumPeerConfig = new QuorumPeerConfig();
    quorumPeerConfig.parseProperties(properties);

    final ServerConfig serverConfig = new ServerConfig();
    serverConfig.readFrom(quorumPeerConfig);

    final ZooKeeperServerMain zooKeeperServer = new ZooKeeperServerMain();
    zooKeeperServer.runFromConfig(serverConfig);
//    Thread thread = new Thread() {
//      @Override
//      public void run() {
//        try {
//          zooKeeperServer.runFromConfig(serverConfig);
//        } catch (IOException e) {
//          LOG.error("ZooKeeper Failed", e);
//        }
//      }
//    };
//    thread.start();
  }

  public static void main(String[] args) throws Exception {
    ZookeeperServer server = ZookeeperServerBuilder.create()
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
