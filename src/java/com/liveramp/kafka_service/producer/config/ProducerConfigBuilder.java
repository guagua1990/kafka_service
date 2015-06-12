package com.liveramp.kafka_service.producer.config;

import java.util.List;
import java.util.Properties;
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class ProducerConfigBuilder {

  private final String clientId;

  private Set<String> brokers;
  private String requestAck = "1"; // default request ack
  private long bufferMemory = 64 * 1024 * 1024l; // default buffer size 64 MB
  private String compressionType = "none"; // default compression type
  private int retries = 3; // conceptually we don't care about order, so we want retry if meet a transient failure
  private int batchSize = 10000; // default batch size
  private int timeout = 10 * 1000; // default time out waiting for acks
  private boolean blockOnBufferFull = true; // default to block buffer
  private List<Class<? extends MetricsReporter>> metricsReporters;

  public ProducerConfigBuilder(String clientId) {
    this.clientId = clientId;
    this.brokers = Sets.newHashSet();
    this.metricsReporters = Lists.newArrayList();
  }

  public ProducerConfigBuilder addBroker(String hostname, int port) {
    brokers.add(Joiner.on(":").join(hostname, port));
    return this;
  }

  public ProducerConfigBuilder noGuaranteeOfRecordsSent() {
    requestAck = "0";
    return this;
  }

  public ProducerConfigBuilder fullGuaranteeOfRecordsSent() {
    requestAck = "all";
    return this;
  }

  public ProducerConfigBuilder setBufferSize(long size) {
    bufferMemory = size;
    return this;
  }

  public ProducerConfigBuilder setCompressionToGZip() {
    compressionType = "gzip";
    return this;
  }

  public ProducerConfigBuilder setCompressionToSnappy() {
    compressionType = "snappy";
    return this;
  }

  public ProducerConfigBuilder setRetries(int retries) {
    this.retries = retries;
    return this;
  }

  public ProducerConfigBuilder setBatchSize(int size) {
    this.batchSize = size;
    return this;
  }

  public ProducerConfigBuilder setTimeout(int timeout) {
    this.timeout = timeout;
    return this;
  }

  public ProducerConfigBuilder setBlockOnBufferFull(boolean enable) {
    this.blockOnBufferFull = enable;
    return this;
  }

  public ProducerConfigBuilder addMetricsReporter(Class<? extends MetricsReporter> metricsReporter) {
    this.metricsReporters.add(metricsReporter);
    return this;
  }

  public Properties build() {
    Preconditions.checkArgument(!brokers.isEmpty(), "No broker specified");
    Properties properties = new Properties();

    properties.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Joiner.on(",").join(brokers));
    properties.put(ProducerConfig.ACKS_CONFIG, requestAck);
    properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferMemory);
    properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
    properties.put(ProducerConfig.RETRIES_CONFIG, retries);
    properties.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
    properties.put(ProducerConfig.TIMEOUT_CONFIG, timeout);
    properties.put(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG, timeout);
    properties.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, blockOnBufferFull);

    if (!metricsReporters.isEmpty()) {
      properties.put(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, metricsReporters.getClass());
    }

    return properties;
  }

  public static Properties createConfig(String producerName, String brokers, int timeout) {
    ProducerConfigBuilder builder = new ProducerConfigBuilder(producerName);
    for (String broker : brokers.split(",")) {
      String[] hostport = broker.split(":");
      builder.addBroker(hostport[0], Integer.valueOf(hostport[1]));
    }

    return builder.setTimeout(timeout).build();
  }
}
