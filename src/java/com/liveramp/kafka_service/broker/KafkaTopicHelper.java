package com.liveramp.kafka_service.broker;

import java.util.Properties;
import java.util.Set;

import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import kafka.admin.AdminUtils;
import kafka.admin.TopicCommand;
import kafka.common.TopicExistsException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.Iterator;

import com.liveramp.kafka_service.zookeeper.ZKEnv;
import com.liveramp.kafka_service.zookeeper.ZookeeperClient;

public class KafkaTopicHelper {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaTopicHelper.class);
  private static final int DEFAULT_PARTITIONS = 1;
  private static final int DEFAULT_REPLICATION_FACTOR = 1;

  private ZookeeperClient zookeeperClient;

  public KafkaTopicHelper(ZookeeperClient zookeeperClient) {
    this.zookeeperClient = zookeeperClient;
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

  public String getDescriptions(String topic) {
    return null;
  }

  public Set<String> getTopics() {
    Set<String> topics = Sets.newHashSet();
    Iterator<Tuple2<String, Properties>> it = AdminUtils.fetchAllTopicConfigs(zookeeperClient.get()).iterator();
    while (it.hasNext()) {
      topics.add(it.next()._1());
    }
    return topics;
  }

  public static void main(String[] args) {
    ZookeeperClient zookeeperClient = new ZookeeperClient.Builder(ZKEnv.TEST_ZKS).build();
    KafkaTopicHelper topicHelper = new KafkaTopicHelper(zookeeperClient);
    topicHelper.createTopic("testing-2");
    System.out.println(Joiner.on(", ").join(topicHelper.getTopics()));
  }
}
