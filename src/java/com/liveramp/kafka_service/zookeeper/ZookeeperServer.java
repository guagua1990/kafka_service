package com.liveramp.kafka_service.zookeeper;

import java.io.FileReader;
import java.util.Map;
import java.util.Properties;

import clojure.lang.Obj;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;
import org.jvyaml.YAML;
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

    Map map = (Map)YAML.load(new FileReader("config/zookeeper-server.yaml"));
    final ZookeeperServerBuilder serverBuilder = ZookeeperServerBuilder.create()
        .setClientPort(((Number)map.get("clientPort")).intValue())
        .setDataDir((String)map.get("dataDir"))
        .setDataLogDir((String)map.get("dataLogDir"));

    for (Object entry : map.entrySet()) {
      String key = (String)((Map.Entry)entry).getKey();
      if (key.contains("server")) {
        serverBuilder.addServer(key, (String)((Map.Entry)entry).getValue());
      }
    }

    try {
      serverBuilder.build().start();
    } catch (Exception e) {
      alertHandler.sendAlert("Exception in ZooKeeper", e,
          AlertRecipients.of("yjin@liveramp.com"), AlertRecipients.of("ltu@liveramp.com"), AlertRecipients.of("syan@liveramp.com"));
    }
  }
}
