package com.liveramp.kafka_service.server;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import kafka.admin.AdminUtils;
import kafka.admin.TopicCommand;
import kafka.common.TopicExistsException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.liveramp.kafka_service.zookeeper.ZookeeperClient;

public class KafkaTopicHelper {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaTopicHelper.class);
  private static final int DEFAULT_PARTITIONS = 1;
  private static final int DEFAULT_REPLICATION_FACTOR = 1;

  private ZookeeperClient zookeeperClient;

  private KafkaTopicHelper(ZookeeperClient zookeeperClient) {
    this.zookeeperClient = zookeeperClient;
  }

  public static KafkaTopicHelper create(ZookeeperClient zookeeperClient) {
    return new KafkaTopicHelper(zookeeperClient);
  }

  public void createTopic(String topic) {
    createTopic(topic, DEFAULT_PARTITIONS, DEFAULT_REPLICATION_FACTOR);
  }

  public void createTopic(String topic, int partitions, int replicationFactor) {
    String[] arguments = new String[]{
        "--create",
        "--topic", topic,
        "--partitions", String.valueOf(partitions),
        "--replication-factor", String.valueOf(replicationFactor)
    };
    TopicCommand.TopicCommandOptions options = new TopicCommand.TopicCommandOptions(arguments);
    System.out.println(Joiner.on(" ").join(arguments));
    try {
      TopicCommand.createTopic(zookeeperClient.get(), options);
    } catch (TopicExistsException e) {
      LOG.error("Topic {} already exists", topic);
    }
  }

  public void deleteTopic(String topic) {
    try {
      AdminUtils.deleteTopic(zookeeperClient.get(), topic);
    } catch (ZkNodeExistsException e) {
      LOG.error("Topic {} has already been deleted", topic);
    }
  }

  public Set<String> getTopics() {
    PrintStream original = System.out;

    ByteArrayOutputStream redirection = new ByteArrayOutputStream();
    PrintStream ps = new PrintStream(redirection);
    System.setOut(ps);

    String[] arguments = new String[]{
        "--list",
    };
    TopicCommand.TopicCommandOptions options = new TopicCommand.TopicCommandOptions(arguments);
    TopicCommand.listTopics(zookeeperClient.get(), options);

    System.setOut(original);

    String topicOutput = redirection.toString();
    Set<String> topics = Sets.newHashSet();
    for (String topic : topicOutput.split("\n")) {
      topics.add(topic.trim());
    }

    return topics;
  }
}
