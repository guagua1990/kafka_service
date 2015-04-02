package com.liveramp.kafka_service.zookeeper;

import java.util.Properties;

import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.java_support.alerts_handler.AlertsHandlers;
import com.liveramp.java_support.alerts_handler.recipients.AlertRecipients;
import com.liveramp.java_support.logging.LoggingHelper;
import com.liveramp.kafka_service.server.KafkaBroker;

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
    LoggingHelper.setLoggingProperties("zookeeper");
    AlertsHandler alertHandler = AlertsHandlers.distribution(KafkaBroker.class);

    final ZookeeperServer server = ZookeeperServerBuilder.create()
        .setClientPort(2181)
        .setInitLimit(10)
        .setSyncLimit(5)
        .setDataDir("/tmp/yjin/zookeeper/data")
        .setDataLogDir("/tmp/yjin/zookeeper/log")
        .addServer("0", "s2s-data-syncer00:2888:3888")
        .addServer("1", "s2s-data-syncer01:2888:3888")
        .addServer("2", "s2s-data-syncer02:2888:3888")
        .addServer("3", "s2s-data-syncer03:2888:3888")
        .addServer("4", "s2s-data-syncer04:2888:3888")
        .build();

    try {
      server.start();
    } catch (Exception e) {
      alertHandler.sendAlert("Exception in ZooKeeper", e,
          AlertRecipients.of("yjin@liveramp.com"), AlertRecipients.of("ltu@liveramp.com"), AlertRecipients.of("syan@liveramp.com"));
    }
  }
}
