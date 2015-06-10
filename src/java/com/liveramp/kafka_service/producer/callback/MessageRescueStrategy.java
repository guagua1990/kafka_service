package com.liveramp.kafka_service.producer.callback;

import org.apache.kafka.clients.producer.ProducerRecord;

public interface MessageRescueStrategy<K, V> {

  void rescue(Exception e, ProducerRecord<K, V> record);
}
