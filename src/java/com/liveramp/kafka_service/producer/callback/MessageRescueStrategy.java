package com.liveramp.kafka_service.producer.callback;

public interface MessageRescueStrategy<V> {

  void rescue(Exception e, String topic, V message);
}
