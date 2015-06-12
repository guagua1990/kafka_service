package com.liveramp.kafka_service.integration_tests;

import java.util.Properties;

import com.liveramp.kafka_service.producer.StringProducer;
import com.liveramp.kafka_service.producer.config.ProducerConfigBuilder;

public class ProducerStressTest {

  private final StringProducer producer;

  public ProducerStressTest(Properties properties) {
    this.producer = new StringProducer(properties);
  }

  public static void main(String[] args) {
    Properties properties = new ProducerConfigBuilder("integration-producer")
        .addBroker("s2s-data-syncer00", 9092)
        .addBroker("s2s-data-syncer01", 9092)
        .addBroker("s2s-data-syncer02", 9092)
        .addBroker("s2s-data-syncer03", 9092)
        .addBroker("s2s-data-syncer04", 9092)
        .setTimeout(1000)
        .build();


  }

}
