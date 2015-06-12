package com.liveramp.kafka_service.producer;

import java.util.Properties;

import com.liveramp.kafka_service.producer.callback.DiscardedMessageRescueStrategy;
import com.liveramp.kafka_service.producer.callback.MessageRescueStrategy;

public class ByteArrayProducer extends BaseProducer<byte[], byte[]> {

  private static DiscardedMessageRescueStrategy<byte[]> messageRescueStrategy = new DiscardedMessageRescueStrategy<>();

  public ByteArrayProducer(Properties config) {
    super(config);
  }

  @Override
  protected byte[] serializeKey(String topic, byte[] key) {
    return key;
  }

  @Override
  protected byte[] serializeValue(String topic, byte[] value) {
    return value;
  }

  @Override
  protected MessageRescueStrategy<byte[]> getRescueStrategy() {
    return messageRescueStrategy;
  }
}
