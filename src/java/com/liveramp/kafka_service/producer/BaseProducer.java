package com.liveramp.kafka_service.producer;

import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.kafka_service.producer.callback.MessageRescueStrategy;

public abstract class BaseProducer<K, V> {
  private static final Logger LOG = LoggerFactory.getLogger(BaseProducer.class);
  private final KafkaProducer<byte[], byte[]> producer;

  protected BaseProducer(Properties config) {
    this.producer = new KafkaProducer<>(config);
  }

  public void send(String topic, V message) {
    send(topic, null, message);
  }

  public void send(String topic, K key, V message) {
    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic,
        serializeKey(topic, key),
        serializeValue(topic, message));
    producer.send(record, new ProducerCallback(getRescueStrategy(), record));
  }

  public List<PartitionInfo> getPartitionForTopic(String topic) {
    return producer.partitionsFor(topic);
  }

  public Metric metrics(MetricName metricName) {
    return producer.metrics().get(metricName);
  }

  public void close() {
    producer.close();
  }

  protected abstract byte[] serializeKey(String topic, K key);

  protected abstract byte[] serializeValue(String topic, V value);

  protected abstract MessageRescueStrategy<K, V> getRescueStrategy();

  protected static class ProducerCallback<K, V> implements Callback {

    private final MessageRescueStrategy<K, V> messageRescueStrategy;
    private final ProducerRecord<K, V> record;

    private ProducerCallback(MessageRescueStrategy<K, V> messageRescueStrategy, ProducerRecord<K, V> record) {
      this.messageRescueStrategy = messageRescueStrategy;
      this.record = record;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
      if (exception != null && messageRescueStrategy != null) {
        messageRescueStrategy.rescue(exception, record);
      }
    }
  }
}
