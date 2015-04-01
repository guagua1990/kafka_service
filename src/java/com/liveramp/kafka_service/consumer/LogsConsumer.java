package com.liveramp.kafka_service.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.liveramp.kafka_service.producer.config.SyncProducerConfigBuilder;
import com.liveramp.kafka_service.producer.serializer.DefaultStringEncoder;

public class LogsConsumer extends Thread {

  final static String READ_TOPIC = "**";
  final static String MERGER_TOPIC = "**";
  final ConsumerConnector consumerConnector;
  final StatsSummer statsSummer = new StatsSummer();
  final Producer<String, String> producer;
  final ScheduledNotifier timer;

  public static void main(String[] args) {
    LogsConsumer logConsumer =new LogsConsumer();
    logConsumer.start();
  }

  public LogsConsumer() {
    consumerConnector = getConsumerConnector();
    producer = getProducer();
    timer = new ScheduledNotifier(this);
  }

  @Override
  public void run() {
    ConsumerIterator<byte[], byte[]> it = getMsgIter();
    timer.start();

    try {
      while (it.hasNext()) {
        String jsonStr = new String(it.next().message());
        statsSummer.summJson(jsonStr);
        // this is a very cheap check whether timer has send a signal.
        // it will reset the interrupt flag.
        if (interrupted()) {
          sendStat(statsSummer.getStatsJsonStrings());
          statsSummer.clear();
        }
      }
    } catch (Exception e) {
      timer.cancel();
      producer.close();
      consumerConnector.shutdown();
      throw new RuntimeException(e);
    }
  }

  private static ConsumerConnector getConsumerConnector() {
    Properties properties = new Properties();
    properties.put("zookeeper.connect","localhost:2181");
    properties.put("group.id","test-group");
    ConsumerConfig consumerConfig = new ConsumerConfig(properties);
    return Consumer.createJavaConsumerConnector(consumerConfig);
  }

  private static Producer<String, String> getProducer() {
    ProducerConfig config = new SyncProducerConfigBuilder(new DefaultStringEncoder())
        .addBroker("localhost", 9092)
        .build();
    return new Producer<String, String>(config);
  }

  private ConsumerIterator<byte[], byte[]> getMsgIter() {
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(READ_TOPIC, 1);
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
    KafkaStream<byte[], byte[]> stream =  consumerMap.get(READ_TOPIC).get(0);
    return stream.iterator();
  }

  private void sendStat(List<String> jsonStats) {
    for (String jsonStat : jsonStats) {
      KeyedMessage<String, String> data = new KeyedMessage<String, String>(MERGER_TOPIC, jsonStat);
      producer.send(data);
    }
  }
}
