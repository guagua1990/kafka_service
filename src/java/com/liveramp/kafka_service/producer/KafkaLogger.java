package com.liveramp.kafka_service.producer;

import java.util.Properties;

import com.rapleaf.spruce_lib.log.EntryLogger;
import com.rapleaf.spruce_lib.log.SpruceLogEntry;

public class KafkaLogger implements EntryLogger {

  private final StringProducer producer;
  private final Double perfLogRate;

  public KafkaLogger(Properties config, Double perfLogRate) {
    this.producer = new StringProducer(config);
    this.perfLogRate = perfLogRate;
  }

  @Override
  public void writeLogEntry(SpruceLogEntry spruceLogEntry) {
    if (spruceLogEntry != null) {
      producer.send(spruceLogEntry.getCategory(), spruceLogEntry.toString());
    }
  }

  @Override
  public void writeStringAndCategory(String s, String s2) {
    if (s != null && s2 != null) {
      producer.send(s, s2);
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
