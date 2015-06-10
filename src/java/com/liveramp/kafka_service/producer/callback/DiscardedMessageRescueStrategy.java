package com.liveramp.kafka_service.producer.callback;

import org.apache.kafka.clients.producer.ProducerRecord;

public class DiscardedMessageRescueStrategy<K, V> implements MessageRescueStrategy<K, V> {

  @Override
  public void rescue(Exception e, ProducerRecord<K, V> record) {
    // just discard the message
    System.out.println("Meet exception " + e.getMessage());
    System.out.println(String.format("Discarded 1 message {%s} for topic %s", record.value(), record.topic()));
  }
}
