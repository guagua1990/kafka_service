package com.liveramp.kafka_service.producer;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.liveramp.kafka_service.producer.callback.DiscardedMessageRescueStrategy;
import com.liveramp.kafka_service.producer.callback.MessageRescueStrategy;
import com.liveramp.kafka_service.producer.config.ProducerConfigBuilder;

public class StringProducer extends BaseProducer<String, String> {

  private final Serializer<String> serializer;
  private final MessageRescueStrategy<String, String> messageRescueStrategy;

  public StringProducer(Properties properties) {
    super(properties);
    serializer = new StringSerializer();
    messageRescueStrategy = new DiscardedMessageRescueStrategy<>();
  }

  @Override
  protected byte[] serializeKey(String topic, String key) {
    return serializer.serialize(topic, key);
  }

  @Override
  protected byte[] serializeValue(String topic, String value) {
    return serializer.serialize(topic, value);
  }

  @Override
  protected MessageRescueStrategy<String, String> getRescueStrategy() {
    return messageRescueStrategy;
  }

  public static Properties createConfig(String producerName, String brokers, int timeout) {
    ProducerConfigBuilder builder = new ProducerConfigBuilder(producerName);
    for (String broker : brokers.split(",")) {
      String[] hostport = broker.split(":");
      builder.addBroker(hostport[0], Integer.valueOf(hostport[1]));
    }

    return builder.setTimeout(timeout).build();
  }

  public static void main(String[] args) throws IOException {
    if (args.length < 3) {
      System.out.println("Usage: <brokerhost:port,broker:port...> <topic> <messages...>");
      return;
    }

    StringProducer producer = new StringProducer(createConfig("simple-string-producer", args[0], 100));

    String topic = args[1];
    for (int i = 2; i < args.length; i++) {
      producer.send(topic, args[i]);
    }
    producer.close();
  }
}
