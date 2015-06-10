package com.liveramp.kafka_service.broker;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.Set;

import com.google.common.collect.Sets;
import kafka.admin.AdminUtils;
import kafka.api.TopicMetadata;
import kafka.common.TopicExistsException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;

import com.liveramp.java_support.logging.LoggingHelper;
import com.liveramp.kafka_service.zookeeper.ZookeeperClient;
import com.liveramp.kafka_service.zookeeper.ZookeeperEnv;

public class TopicHelper {
  private static final Logger LOG = LoggerFactory.getLogger(TopicHelper.class);
  private static final int DEFAULT_PARTITIONS = 1;
  private static final int DEFAULT_REPLICATION_FACTOR = 1;

  static {
    LoggingHelper.setLoggingProperties("topic");
  }

  private TopicHelper() {
    throw new AssertionError("Don't instantiate the class");
  }

  public static void createTopic(ZookeeperClient client, String topic) {
    createTopic(client, topic, DEFAULT_PARTITIONS, DEFAULT_REPLICATION_FACTOR);
  }

  public static void createTopic(ZookeeperClient client, String topic, int partitions, int replicationFactor) {
    try {
      AdminUtils.createTopic(client.get(), topic, partitions, replicationFactor, new Properties());
    } catch (TopicExistsException e) {
      LOG.error("Topic {} already exists", topic);
    }
  }

  public static void deleteTopic(ZookeeperClient client, String topic) {
    try {
      AdminUtils.deleteTopic(client.get(), topic);
    } catch (ZkNodeExistsException e) {
      LOG.error("Topic {} has already been deleted", topic);
    }
  }

  public static TopicMetadata getDescription(ZookeeperClient client, String topic) {
    return AdminUtils.fetchTopicMetadataFromZk(topic, client.get());
  }

  public static Set<String> getAllTopics(ZookeeperClient client) {
    Set<String> topics = Sets.newHashSet();
    Iterator<String> iter = AdminUtils.fetchAllTopicConfigs(client.get()).keySet().iterator();
    while (iter.hasNext()) {
      topics.add(iter.next());
    }
    return topics;
  }

  public static void main(String[] args) throws IOException {
    ZookeeperClient client = new ZookeeperClient.Builder(ZookeeperEnv.getZKInstances()).build();
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

    System.out.println("Please enter the topic command:");
    System.out.print("kafka >");
    String command = null;
    while ((command = br.readLine()) != null) {
      if (command.startsWith("create")) {
        String[] ops = command.split(" ");
        if (ops.length < 1) {
          System.out.println("Usage: create topic <num_partition> <num_replica>");
        }
        String topic = ops[1];
        int partitions = ops.length > 1 ? Integer.valueOf(ops[2]) : 1;
        int replicas = ops.length > 2 ? Integer.valueOf(ops[3]) : 1;
        createTopic(client, topic, partitions, replicas);
        System.out.println("Created topic " + topic);
      } else if (command.equals("list")) {
        System.out.println(getAllTopics(client));
      } else if (command.startsWith("delete")) {
        String topic = command.split(" ")[1];
        deleteTopic(client, topic);
        System.out.println("Deleted topic " + topic);
      } else if (command.startsWith("describe")) {
        String topic = command.split(" ")[1];
        System.out.println(getDescription(client, topic));
      } else if (command.equals("exit")) {
        break;
      } else {
        System.out.println("No such command: " + command);
      }
      System.out.print("kafka >");
    }
    client.close();
  }

}
