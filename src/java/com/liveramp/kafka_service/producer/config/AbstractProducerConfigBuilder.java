package com.liveramp.kafka_service.producer.config;

import java.util.Properties;
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import kafka.producer.Partitioner;
import kafka.producer.ProducerConfig;
import kafka.serializer.Encoder;

public abstract class AbstractProducerConfigBuilder {

  public static enum ProducerType {
    SYNC("sync"),
    ASYNC("async");

    private final String value;

    private ProducerType(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }
  }

  private final ProducerType type;
  private final Set<String> brokers;
  private final Encoder serializer;
  private Partitioner partitioner;

  public AbstractProducerConfigBuilder(ProducerType type, Encoder serializer) {
    this.type = type;
    this.brokers = Sets.newHashSet();
    this.serializer = serializer;
  }

  public AbstractProducerConfigBuilder addBroker(String hostname, int port) {
    brokers.add(Joiner.on(":").join(hostname, port));
    return this;
  }

  public AbstractProducerConfigBuilder setPartitioner(Partitioner partitioner) {
    this.partitioner = partitioner;
    return this;
  }

  public Properties getBasicProperties() {
    Preconditions.checkArgument(!brokers.isEmpty(), "brokers must not be empty");
    Preconditions.checkNotNull(serializer, "serializer class must not be null");

    Properties properties = new Properties();

    properties.put("producer.type", type.getValue());
    properties.put("metadata.broker.list", Joiner.on(",").join(brokers));
    properties.put("serializer.class", serializer.getClass().getCanonicalName());
    properties.put("request.required.acks", "1");

    if (partitioner != null) {
      properties.put("partitioner.class", partitioner.getClass().getCanonicalName());
    }

    return properties;
  }

  public abstract ProducerConfig build();
}
