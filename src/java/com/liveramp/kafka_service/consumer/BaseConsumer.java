package com.liveramp.kafka_service.consumer;

import java.util.Map;
import java.util.Properties;
import java.util.Queue;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.kafka_service.consumer.persist_helpers.KafkaOffsetHelper;
import com.liveramp.kafka_service.consumer.rebalance_callbacks.BaseRebalanceCallback;

public abstract class BaseConsumer<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(BaseConsumer.class);
  private static final long DEFAULT_TIMEOUT = 3000;

  private final KafkaOffsetHelper offsetHelper;
  private final KafkaConsumer<K, V> consumer;
  private final Map<String, Queue<ConsumerRecord<K, V>>> cache;

  protected BaseConsumer(Properties config, KafkaOffsetHelper offsetHelper) {
    config.setProperty("enable.auto.commit", "false");
    BaseRebalanceCallback baseRebalanceCallback = BaseRebalanceCallback.create(offsetHelper);
    this.offsetHelper = offsetHelper;
    this.consumer = new KafkaConsumer<>(config, baseRebalanceCallback);
    this.cache = Maps.newHashMap();
  }

  public void subscribe(String topic) {
    consumer.subscribe(topic);
    cache.put(topic, Queues.<ConsumerRecord<K, V>>newArrayDeque());
  }

  public void unsubscription(String topic) {
    consumer.unsubscribe(topic);
    cache.remove(topic);
  }

  public void close() {
    consumer.close();
  }

  public boolean hasNext(String topic) {
    if (!cache.containsKey(topic)) {
      return false;
    }

    if (cache.get(topic).size() > 0) {
      return true;
    }

    Map<String, ConsumerRecords<K, V>> topicToRecordsMap = consumer.poll(DEFAULT_TIMEOUT);
    for (Map.Entry<String, ConsumerRecords<K, V>> topicToRecords : topicToRecordsMap.entrySet()) {
      cache.get(topicToRecords.getKey()).addAll(topicToRecords.getValue().records());
    }

    return cache.get(topic).size() > 0;
  }

  public V next(String topic) {
    ConsumerRecord<K, V> record = cache.get(topic).remove();

    try {
      offsetHelper.persistOffset(record.topicAndPartition(), record.offset());
      return record.value();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
