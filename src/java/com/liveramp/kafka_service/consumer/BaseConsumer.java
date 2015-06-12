package com.liveramp.kafka_service.consumer;

import java.util.Map;
import java.util.Properties;

import com.google.common.collect.Maps;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.kafka_service.consumer.persist_helpers.KafkaOffsetHelper;

public abstract class BaseConsumer<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(BaseConsumer.class);

  private final ConsumerConnector connector;
  private final Map<String, ConsumerIterator<byte[], byte[]>> topicStreamMap;
  private final KafkaOffsetHelper offsetHelper;
  private final Deserializer<V> deserializer;

  protected BaseConsumer(Properties config, KafkaOffsetHelper offsetHelper, Deserializer<V> deserializer) {
    this.connector = Consumer.createJavaConsumerConnector(new ConsumerConfig(config));
    this.topicStreamMap = Maps.newHashMap();
    this.offsetHelper = offsetHelper;
    this.deserializer = deserializer;
  }

  public void subscribe(String topic) {
    Map<String, Integer> topicCountMap = Maps.newHashMap();
    topicCountMap.put(topic, 1);
    topicStreamMap.put(topic, connector.createMessageStreams(topicCountMap).get(topic).get(0).iterator());
  }

  public void unsubscription(String topic) {
    topicStreamMap.remove(topic);
  }

  public void close() {
    connector.shutdown();
  }

  public boolean hasNext(String topic) {
    return topicStreamMap.get(topic).hasNext();
  }

  public V next(String topic) {
    MessageAndMetadata<byte[], byte[]> record = topicStreamMap.get(topic).next();
    offsetHelper.persistOffset(new TopicPartition(topic, record.partition()), record.offset());
    return deserializer.deserialize(topic, record.message());
  }
}
