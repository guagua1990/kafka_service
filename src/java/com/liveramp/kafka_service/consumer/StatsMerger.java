package com.liveramp.kafka_service.consumer;

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
import org.json.JSONObject;
import org.jvyaml.YAML;

import com.liveramp.kafka_service.consumer.utils.JsonFactory;
import com.liveramp.kafka_service.consumer.utils.StatsSummer;

public class StatsMerger extends Thread {

  private final ConsumerConnector consumerConnector;
  private final StatsSummer statsSummer;

  public static void main(String[] args) throws FileNotFoundException {
    StatsMerger logConsumer = new StatsMerger();
    logConsumer.start();
  }

  public StatsMerger() throws FileNotFoundException {
    consumerConnector = getConsumerConnector();
    statsSummer = new StatsSummer();
  }

  @Override
  public void run() {
    ConsumerIterator<byte[], byte[]> it = getMsgIter();

    try {
      while (it.hasNext()) {
        String statsEntry = new String(it.next().message());
        JSONObject object = new JSONObject(statsEntry);
        JsonFactory.StatsType statsType = JsonFactory.StatsType.valueOf(object.getString(JsonFactory.STATS_TYPE));
        String statJsonString = object.getString(JsonFactory.STAT);

        double updatedResult = statsSummer.summStatJson(statsType, statJsonString);
        updateDb(updatedResult, statJsonString);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void updateDb(double updatedResult, String statJsonString) {
    // TODO: grab information from statJsonString and send it to db.

  }

  private static ConsumerConnector getConsumerConnector() throws FileNotFoundException {
    Properties properties = new Properties();
    Map map = (Map)YAML.load(new FileReader("config/zookeeper-client.yaml"));
    properties.put("zookeeper.connect", map.get("zookeeper.connect"));
    properties.put("group.id","test-par-1");
    ConsumerConfig consumerConfig = new ConsumerConfig(properties);
    return Consumer.createJavaConsumerConnector(consumerConfig);
  }

  private ConsumerIterator<byte[], byte[]> getMsgIter() {
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(ConsumerConstants.MERGER_TOPIC, 1);
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
    KafkaStream<byte[], byte[]> stream =  consumerMap.get(ConsumerConstants.MERGER_TOPIC).get(0);
    return stream.iterator();
  }

  private void sendStat(List<String> jsonStats) {
  }
}
