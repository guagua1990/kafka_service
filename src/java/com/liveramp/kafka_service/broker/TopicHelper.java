package com.liveramp.kafka_service.broker;

import java.util.Set;

import com.google.common.collect.Sets;
import kafka.admin.AdminUtils;
import kafka.admin.TopicCommand;
import kafka.api.TopicMetadata;
import kafka.common.TopicExistsException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;

import com.liveramp.kafka_service.zookeeper.ZookeeperClient;

public class TopicHelper {
  private static final Logger LOG = LoggerFactory.getLogger(TopicHelper.class);
  private static final int DEFAULT_PARTITIONS = 1;
  private static final int DEFAULT_REPLICATION_FACTOR = 1;

  private TopicHelper() {
    throw new AssertionError("Don't instantiate the class");
  }

  public static void createTopic(ZookeeperClient client, String topic) {
    createTopic(client, topic, DEFAULT_PARTITIONS, DEFAULT_REPLICATION_FACTOR);
  }

  public static void createTopic(ZookeeperClient client, String topic, int partitions, int replicationFactor) {
    String[] arguments = new String[]{
        "--create",
        "--topic", topic,
        "--partitions", String.valueOf(partitions),
        "--replication-factor", String.valueOf(replicationFactor)
    };
    TopicCommand.TopicCommandOptions options = new TopicCommand.TopicCommandOptions(arguments);
    try {
      TopicCommand.createTopic(client.get(), options);
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

}
