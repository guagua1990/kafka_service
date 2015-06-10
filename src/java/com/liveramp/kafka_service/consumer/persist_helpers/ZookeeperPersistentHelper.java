package com.liveramp.kafka_service.consumer.persist_helpers;

import java.util.Collection;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.kafka.common.TopicPartition;

import com.liveramp.kafka_service.zookeeper.ZookeeperEnv;
import com.liveramp.kafka_service.zookeeper.ZookeeperClient;

public class ZookeeperPersistentHelper implements PersistentHelper {

  private final ZookeeperClient zookeeperClient;

  private ZookeeperPersistentHelper(final ZookeeperClient zookeeperClient) {
    this.zookeeperClient = zookeeperClient;
  }

  public static ZookeeperPersistentHelper createProductionHelper() {
    return new ZookeeperPersistentHelper(new ZookeeperClient.Builder(ZookeeperEnv.PRODUCTION_ZKS).build());
  }

  public static ZookeeperPersistentHelper createTestHelper() {
    return new ZookeeperPersistentHelper(new ZookeeperClient.Builder(ZookeeperEnv.TEST_ZKS).build());
  }

  @Override
  public boolean persistOffset(final TopicPartition partition, final long offset) {
    return zookeeperClient.deleteNode(partition.toString(), true) && zookeeperClient.createNode(partition.toString() + "/" + offset);
  }

  @Override
  public boolean persistOffsets(final Map<TopicPartition, Long> topicPartitionOffsets) {
    boolean success = true;
    for (Map.Entry<TopicPartition, Long> entry : topicPartitionOffsets.entrySet()) {
      success = success && persistOffset(entry.getKey(), entry.getValue());
    }
    return success;
  }

  @Override
  public Long retrieveOffset(final TopicPartition partition) {
    return Long.valueOf(zookeeperClient.readNode(partition.toString()).get(0));
  }

  @Override
  public Map<TopicPartition, Long> retrieveOffsets(final Collection<TopicPartition> partitions) {
    Map<TopicPartition, Long> topicPartitionOffsets = Maps.newHashMap();

    for (TopicPartition partition : partitions) {
      topicPartitionOffsets.put(partition, retrieveOffset(partition));
    }

    return topicPartitionOffsets;
  }
}
