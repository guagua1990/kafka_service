package com.liveramp.kafka_service.producer.config;

import kafka.producer.ProducerConfig;
import kafka.serializer.Encoder;

public class SyncProducerConfigBuilder extends AbstractProducerConfigBuilder {

  public SyncProducerConfigBuilder(Encoder serializer) {
    super(ProducerType.SYNC, serializer);
  }

  @Override
  public ProducerConfig build() {
    return new ProducerConfig(getBasicProperties());
  }
}
