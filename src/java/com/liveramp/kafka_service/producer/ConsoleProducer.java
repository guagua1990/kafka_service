package com.liveramp.kafka_service.producer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

import com.liveramp.kafka_service.broker.TopicHelper;
import com.liveramp.kafka_service.zookeeper.ZookeeperClient;
import com.liveramp.kafka_service.zookeeper.ZookeeperEnv;

public class ConsoleProducer extends StringProducer {

  public ConsoleProducer(Properties properties) {
    super(properties);
  }

  public static void main(String[] args) throws IOException {
    if (args.length < 1) {
      System.out.println("Usage: broker:port,broker:port...");
      return;
    }
    ZookeeperClient client = new ZookeeperClient.Builder(ZookeeperEnv.getZKInstances()).build();
    System.out.println("Current available topics on kafka:");
    System.out.println(TopicHelper.getAllTopics(client));
    StringProducer producer = new StringProducer(createConfig("console-producer", args[0], 1000));

    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

    System.out.print("Please enter the topic you want to send: ");
    String topic = br.readLine();
    System.out.println("Please enter the message you want to send to [topic] " + topic + ":");
    String line = null;
    while ((line = br.readLine()) != null) {
      if (line.equals("exit")) {
        break;
      }
      producer.send(topic, line);
    }
    producer.close();
    System.out.println("Console producer closed.");
  }
}
