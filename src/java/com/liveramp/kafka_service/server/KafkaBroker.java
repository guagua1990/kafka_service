package com.liveramp.kafka_service.server;

import java.util.Properties;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.java_support.alerts_handler.AlertsHandler;
import com.liveramp.java_support.alerts_handler.AlertsHandlers;
import com.liveramp.java_support.alerts_handler.recipients.AlertRecipients;
import com.liveramp.java_support.logging.LoggingHelper;

public class KafkaBroker {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaBroker.class);
  private final int brokerId;
  private final KafkaServer server;

  KafkaBroker(int brokerId, Properties properties, Time time) {
    this.brokerId = brokerId;
    this.server = new KafkaServer(new KafkaConfig(properties), time);
  }

  public void start() {
    server.startup();
  }

  public void shutdown() {
    server.awaitShutdown();
  }

  public static void main(String[] args) {
    if (args.length != 1) {
      System.out.println("Usage: broker_id");
      return;

    }
    int brokerId = Integer.valueOf(args[0]);
    LoggingHelper.setLoggingProperties("broker");
    AlertsHandler alertHandler = AlertsHandlers.distribution(KafkaBroker.class);
    try {
      final KafkaBroker broker = KafkaBrokerBuilder.create(brokerId)
          .setZookeeperConnect("s2s-data-syncer00:2181," +
              "s2s-data-syncer01:2181," +
              "s2s-data-syncer02:2181," +
              "s2s-data-syncer03:2181," +
              "s2s-data-syncer04:2181")
          .setPort(9092)
          .setDeleteTopicEnable(true)
          .setLogDirs("/tmp/yjin/kafka-logs")
          .build();

      broker.start();

      Runtime.getRuntime().addShutdownHook(new ShutdownHook(broker));
    } catch (Exception e) {
      alertHandler.sendAlert("Exception in Kafka Broker " + brokerId, e,
          AlertRecipients.of("yjin@liveramp.com"), AlertRecipients.of("ltu@liveramp.com"), AlertRecipients.of("syan@liveramp.com"));
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
