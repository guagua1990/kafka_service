package com.liveramp.kafka_service.consumer.channels;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.jvyaml.YAML;

import com.liveramp.kafka_service.consumer.ConsumerConstants;
import com.liveramp.kafka_service.producer.AttributionLogGenerator;

public class LogSource implements MessageIterator {

  private final ConsumerIterator<byte[], byte[]> stream;

  private static ConsumerConnector getConsumerConnector() throws FileNotFoundException {
    Properties properties = new Properties();
    Map map = (Map)YAML.load(new FileReader("config/zookeeper-client.yaml"));
    properties.put("zookeeper.connect", map.get("zookeeper.connect"));
    properties.put("group.id", ConsumerConstants.CONSUMER_GROUP);
    ConsumerConfig consumerConfig = new ConsumerConfig(properties);
    return Consumer.createJavaConsumerConnector(consumerConfig);
  }

  public LogSource() throws FileNotFoundException {
    ConsumerConnector consumerConnector = getConsumerConnector();
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(ConsumerConstants.READ_TOPIC, 1);
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
    KafkaStream<byte[], byte[]> stream =  consumerMap.get(ConsumerConstants.READ_TOPIC).get(0);
    this.stream = stream.iterator();
  }

  @Override
  public boolean hasNext() {
    return stream.hasNext();
  }

  @Override
  public String next() {
    String rawStr = new String(stream.next().message());
    return rawStr.split(ConsumerConstants.DELIMITER)[1] + ConsumerConstants.MSG_DELIMITER;
  }

  public static void main(String[] args) throws FileNotFoundException {
    MessageGenerator messageWorker = new MessageGenerator(new LogSource());
    messageWorker.run();
  }
}
