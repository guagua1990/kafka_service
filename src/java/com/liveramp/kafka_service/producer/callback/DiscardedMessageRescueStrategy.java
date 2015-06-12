package com.liveramp.kafka_service.producer.callback;

public class DiscardedMessageRescueStrategy<V> implements MessageRescueStrategy<V> {

  @Override
  public void rescue(Exception e, String topic, V message) {
    // just discard the message
    System.out.println(String.format("Meet [%s] caused by %s", e.getClass().getCanonicalName(), e.getMessage()));
    System.out.println(String.format("Discarded 1 message {%s} for topic %s", message, topic));
  }
}
