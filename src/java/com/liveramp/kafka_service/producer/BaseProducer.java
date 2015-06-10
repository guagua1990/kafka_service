package com.liveramp.kafka_service.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serializer;

public abstract class BaseProducer<K, V> {
  private final KafkaProducer<byte[], byte[]> producer;
  private final Callback callback;

  protected BaseProducer(Properties config) {
    this(config, new ProducerCallBack());
  }

  protected BaseProducer(Properties config, Callback callback) {
    this.producer = new KafkaProducer<>(config);
    this.callback = callback;
  }

  public void send(String topic, K key, V message) {
    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic,
        getKeySerializer().serialize(topic, key),
        getValueSerializer().serialize(topic, message));
    producer.send(record, callback);
  }

  public void close() {
    producer.close();
  }

  protected abstract Serializer<K> getKeySerializer();

  protected abstract Serializer<V> getValueSerializer();

  public static class ProducerCallBack implements Callback {

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
    }
  }
}
