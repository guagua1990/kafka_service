package com.liveramp.kafka_service.consumer;

import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.kafka_service.consumer.persist_helpers.KafkaOffsetHelper;
import com.liveramp.kafka_service.consumer.rebalance_callbacks.BaseRebalanceCallback;

public abstract class BaseConsumer<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(BaseConsumer.class);

  private final KafkaOffsetHelper offsetHelper;
  private final KafkaConsumer<byte[], byte[]> consumer;

  protected BaseConsumer(Properties config, KafkaOffsetHelper offsetHelper) {
    config.setProperty("enable.auto.commit", "false");
    BaseRebalanceCallback baseRebalanceCallback = BaseRebalanceCallback.create(offsetHelper);
    this.offsetHelper = offsetHelper;
    this.consumer = new KafkaConsumer<>(config, baseRebalanceCallback);
  }

  public void startSubscription(String topic) {
    consumer.subscribe(topic);
  }

  public void stopSubscription(String topic) {
    consumer.unsubscribe(topic);
  }

  public void close() {
    consumer.close();
  }

}
