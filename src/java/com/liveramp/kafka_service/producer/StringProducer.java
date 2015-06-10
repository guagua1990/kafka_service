package com.liveramp.kafka_service.producer;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.liveramp.kafka_service.producer.config.ProducerConfigBuilder;

public class StringProducer extends BaseProducer<String, String> {

  private final Serializer<String> serializer;

  public StringProducer(Properties properties) {
    super(properties);
    serializer = new StringSerializer();
  }

  @Override
  protected Serializer<String> getKeySerializer() {
    return serializer;
  }

  @Override
  protected Serializer<String> getValueSerializer() {
    return serializer;
  }

  public static void main(String[] args) {
    StringProducer producer = new StringProducer(new ProducerConfigBuilder("base")
        .addBroker("localhost", 9092)
        .setTimeout(10)
        .build());
    for (int i = 0; i < 10; i++) {
      System.out.println("hello");
      producer.send("goo", "key", "value" + i);
    }
    producer.close();
  }
}
