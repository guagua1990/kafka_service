package com.liveramp.kafka_service.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * Created by yjin on 1/23/15.
 */
public class BaseProducer<K, V> extends Producer<K, V> {

  public BaseProducer(ProducerConfig config) {
    super(config);
  }

  @Override
  public void send(KeyedMessage<K, V> message) {

  }

}
