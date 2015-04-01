package com.liveramp.kafka_service.producer.config;

import java.util.Properties;

import kafka.producer.ProducerConfig;
import kafka.producer.async.EventHandler;
import kafka.serializer.Encoder;

public class AsyncProducerConfigBuilder extends AbstractProducerConfigBuilder {

  private int queueSize;
  private int batchSize;
  private EventHandler eventHandler;

  public AsyncProducerConfigBuilder(Encoder serializer) {
    super(ProducerType.ASYNC, serializer);
  }


  @Override
  public ProducerConfig build() {
    Properties properties = getBasicProperties();

    properties.put("queue.size", String.valueOf(queueSize));
    properties.put("batch.size", String.valueOf(batchSize));
    properties.put("event.handler", eventHandler.getClass().getCanonicalName());

    return new ProducerConfig(properties);
  }
}
