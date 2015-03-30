package kafka_service.server;

import java.io.IOException;

import kafka.server.AbstractFetcherManager;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.apache.zookeeper.server.quorum.QuorumPeerMain;

public class KafkaServer {

  private AbstractFetcherManager manager;

  public static void main(String[] args) throws IOException, QuorumPeerConfig.ConfigException {
    KafkaServer server = new KafkaServer();

    QuorumPeerMain main = new QuorumPeerMain();
    QuorumPeerConfig config = new QuorumPeerConfig();
    config.parse("config/zookeeper.properties");
    main.runFromConfig(config);
  }
}