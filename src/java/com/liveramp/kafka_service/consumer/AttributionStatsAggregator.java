package com.liveramp.kafka_service.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public class AttributionStatsAggregator implements Runnable {
  private final String TOPIC = "stats_merge";
  private final kafka.javaapi.consumer.ConsumerConnector consumer;

  public AttributionStatsAggregator(ConsumerConfig config) {
    this.consumer = Consumer.createJavaConsumerConnector(config);
  }

  @Override
  public void run() {
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(TOPIC, 1);
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
    KafkaStream<byte[], byte[]> stream = consumerMap.get(TOPIC).get(0);
    ConsumerIterator<byte[], byte[]> it = stream.iterator();
    while (it.hasNext()) {
      System.out.println(new String(it.next().message()));
    }
  }

  public static void main(String[] args) {

  }
}
