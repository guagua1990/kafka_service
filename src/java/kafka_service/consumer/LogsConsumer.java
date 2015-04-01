package kafka_service.consumer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import hello_world.HelloWorldConsumer;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.json.JSONException;

public class LogsConsumer extends Thread {

  final static String TOPIC = "**";
  ConsumerConnector consumerConnector;
  StatsSummer statsSummer = new StatsSummer();

  public static void main(String[] args) throws UnsupportedEncodingException {
    HelloWorldConsumer helloWorldConsumer = new HelloWorldConsumer();
    helloWorldConsumer.start();
  }

  public LogsConsumer() {
    Properties properties = new Properties();
    properties.put("zookeeper.connect","localhost:2181");
    properties.put("group.id","test-group");
    ConsumerConfig consumerConfig = new ConsumerConfig(properties);
    consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
  }

  @Override
  public void run() {
    ConsumerIterator<byte[], byte[]> it = getIter();

    while (it.hasNext()) {
      String jsonStr = new String(it.next().message());
      try {
        statsSummer.summJson(jsonStr);
      } catch (JSONException e) {
        try {
          statsSummer.emitStats();
        } catch (IOException e1) {
          throw new RuntimeException(e1);
        }
      }
    }
  }

  private ConsumerIterator<byte[], byte[]> getIter() {
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(TOPIC, 1);
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
    KafkaStream<byte[], byte[]> stream =  consumerMap.get(TOPIC).get(0);
    return stream.iterator();
  }
}
