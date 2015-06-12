package com.liveramp.kafka_service.broker;

import java.net.InetAddress;
import java.util.EnumSet;
import java.util.Properties;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.java_support.logging.LoggingHelper;
import com.liveramp.kafka_service.zookeeper.ZookeeperEnv;

public class KafkaBroker {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaBroker.class);

  public static final int DEFAULT_PORT = 9092;

  private final int brokerId;
  private final KafkaServer server;

  KafkaBroker(int brokerId, Properties properties, Time time) {
    this.brokerId = brokerId;
    this.server = new KafkaServer(new KafkaConfig(properties), time);
  }

  public void start() {
    LOG.info("Starting kafka broker {}", brokerId);
    server.startup();
    LOG.info("Started kafka broker {}", brokerId);
  }

  public void shutdown() {
    LOG.info("Shutting down kafka broker {}", brokerId);
    server.awaitShutdown();
    LOG.info("Successfully shut down kafka broker {}", brokerId);
  }

  public static class Builder {
    private final int brokerId;
    private final Properties properties;

    private Builder(int brokerId, String hostname, int port) {
      this.brokerId = brokerId;
      this.properties = new Properties();
      setProperty("advertised.host.name", hostname);
      setProperty("port", String.valueOf(port));
      setProperty("num.network.threads", 3);
      setProperty("num.io.threads", 8);
      setProperty("socket.send.buffer.bytes", 102400);
      setProperty("socket.receive.buffer.bytes", 65536);
      setProperty("socket.request.max.bytes", 104857600);
      setProperty("log.dirs", "/tmp/kafka-logs");
      setProperty("num.partitions", 1);
      setProperty("num.recovery.threads.per.data.dir", 1);
      setProperty("log.retention.hours", 168);
      setProperty("log.segment.bytes", 1073741824);
      setProperty("log.retention.check.interval.ms", 300000);
      setProperty("log.cleaner.enable", false);
      setProperty("zookeeper.connection.timeout.ms", 2000);
      setProperty("controlled.shutdown.enabl", true);
      setProperty("broker.id", String.valueOf(brokerId));
    }

    public Builder setLogDirs(String logDirs) {
      return setProperty("log.dirs", logDirs);
    }

    public Builder setDeleteTopicEnable(boolean deleteTopicEnable) {
      return setProperty("delete.topic.enable", deleteTopicEnable);
    }

    public Builder setZookeeperConnect(EnumSet<ZookeeperEnv.ZKEnsembles> zks) {
      return setZookeeperConnect(ZookeeperEnv.getZkClientConnections(zks));
    }

    public Builder setZookeeperConnect(String zkConnect) {
      return setProperty("zookeeper.connect", zkConnect);
    }

    private Builder setProperty(String key, String value) {
      this.properties.setProperty(key, value);
      return this;
    }

    private Builder setProperty(String key, int value) {
      this.properties.setProperty(key, String.valueOf(value));
      return this;
    }

    private Builder setProperty(String key, boolean value) {
      this.properties.setProperty(key, String.valueOf(value));
      return this;
    }

    public KafkaBroker build() {
      return new KafkaBroker(brokerId, properties, new KafkaBrokerTime());
    }
  }

  public static void main(String[] args) {
    if (args.length < 2) {
      System.out.println("Usage: broker_id <port>");
      return;
    }

    int brokerId = Integer.valueOf(args[0]);
    LoggingHelper.setLoggingProperties("broker");
    try {
      String hostname = InetAddress.getLocalHost().getCanonicalHostName();
      int port = args.length > 1 ? Integer.valueOf(args[1]) : DEFAULT_PORT;
      final KafkaBroker broker = new Builder(brokerId, hostname, port)
          .setZookeeperConnect(ZookeeperEnv.getZKInstances())
          .setDeleteTopicEnable(true)
          .setLogDirs("/tmp/kafka-logs/" + brokerId)
          .build();

      broker.start();
      System.out.println(String.format("Started kafka broker %d on %s:%d", brokerId, hostname, port));

      Runtime.getRuntime().addShutdownHook(new ShutdownHook(broker));
    } catch (Exception e) {
      System.out.println("Exception to start broker " + brokerId);
      System.out.println(e);
    }

  }

  private static class ShutdownHook extends Thread {
    private final KafkaBroker broker;

    public ShutdownHook(KafkaBroker broker) {
      this.broker = broker;
    }

    @Override
    public void run() {
      try {
        broker.shutdown();
      } catch (Exception e) {
        LOG.error("meet error when shutdown broker " + broker, e);
      }
    }
  }
}
