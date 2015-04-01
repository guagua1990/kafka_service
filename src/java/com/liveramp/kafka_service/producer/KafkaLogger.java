package com.liveramp.kafka_service.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.rapleaf.spruce_lib.log.EntryLogger;
import com.rapleaf.spruce_lib.log.SpruceLogEntry;

public class KafkaLogger implements EntryLogger {

  private final Producer<String, String> producer;
  private final Double perfLogRate;

  public KafkaLogger(ProducerConfig config, Double perfLogRate) {
    this.producer = new Producer<String, String>(config);
    this.perfLogRate = perfLogRate;
  }

  @Override
  public void writeLogEntry(SpruceLogEntry spruceLogEntry) {
    if (spruceLogEntry != null) {
      KeyedMessage<String, String> keyedMessage = new KeyedMessage<String, String>(spruceLogEntry.getCategory(), spruceLogEntry.toString());
      producer.send(keyedMessage);
    }
  }

  @Override
  public void writeStringAndCategory(String s, String s2) {
    if (s != null && s2 != null) {
      KeyedMessage<String, String> keyedMessage = new KeyedMessage<String, String>(s, s2);
      producer.send(keyedMessage);
    }
  }

  @Override
  public void writePerfLogEntry(SpruceLogEntry spruceLogEntry) {
    if (perfLogRate != null && Math.random() < perfLogRate) {
      writeLogEntry(spruceLogEntry);
    }
  }

  public void close() {
    producer.close();
  }
}
