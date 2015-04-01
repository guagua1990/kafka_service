package hello_world;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import com.liveramp.kafka_service.consumer.config.YamlConsumerConfigBuilder;

public class HelloWorldConsumer extends Thread {
  final static String TOPIC = "new3";
  ConsumerConnector consumerConnector;


  public static void main(String[] argv) throws UnsupportedEncodingException {
    HelloWorldConsumer helloWorldConsumer = new HelloWorldConsumer();
    helloWorldConsumer.start();
  }

<<<<<<< Updated upstream
  public HelloWorldConsumer(){
    Properties properties = new Properties();
    properties.put("zookeeper.connect","10.99.32.1:2181,10.99.32.14:2181,10.99.32.36:2181");
    properties.put("group.id","test-group");
    ConsumerConfig consumerConfig = new ConsumerConfig(properties);
    consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
=======
  public HelloWorldConsumer() {
    consumerConnector = Consumer.createJavaConsumerConnector(YamlConsumerConfigBuilder.buildFromYaml("config/consumer.yaml"));
>>>>>>> Stashed changes
  }

  @Override
  public void run() {
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(TOPIC, 1);
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
    KafkaStream<byte[], byte[]> stream = consumerMap.get(TOPIC).get(0);
    ConsumerIterator<byte[], byte[]> it = stream.iterator();
    while (it.hasNext()) {
      System.out.println(new String(it.next().message()));
    }
  }
}
