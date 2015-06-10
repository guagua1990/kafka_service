package com.liveramp.kafka_service.consumer.persist_helpers;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;

import com.liveramp.kafka_service.zookeeper.ZKEnv;
import com.liveramp.kafka_service.zookeeper.ZookeeperClient;

public class ZookeeperPersistentHelper implements PersistentHelper {

  private final ZookeeperClient zookeeperClient;

  private ZookeeperPersistentHelper(final ZookeeperClient zookeeperClient) {
    this.zookeeperClient = zookeeperClient;
  }

  public static ZookeeperPersistentHelper createProductionHelper() {
    return new ZookeeperPersistentHelper(new ZookeeperClient.Builder(ZKEnv.PRODUCTION_ZKS).build());
  }

  public static ZookeeperPersistentHelper createTestHelper() {
    return new ZookeeperPersistentHelper(new ZookeeperClient.Builder(ZKEnv.TEST_ZKS).build());
  }

  @Override
  public boolean persistOffset(final TopicPartition partition, final long offset) {
    return false;
  }

  @Override
  public boolean persistOffsets(final Map<TopicPartition, Long> topicPartitionOffsets) {
    return false;
  }

  @Override
  public Long retrieveOffset(final TopicPartition partition) {
    return null;
  }

  @Override
  public Map<TopicPartition, Long> retrieveOffsets(final Collection<TopicPartition> partitions) {
    return null;
  }
}
