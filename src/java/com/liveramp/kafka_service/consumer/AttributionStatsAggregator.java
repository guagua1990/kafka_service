package com.liveramp.kafka_service.consumer;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;

public class AttributionStatsAggregator {
  private final kafka.javaapi.consumer.ConsumerConnector consumer;

  public AttributionStatsAggregator(ConsumerConfig config) {
    this.consumer = Consumer.createJavaConsumerConnector(config);

  }

  public static void main(String[] args) {

  }
}
