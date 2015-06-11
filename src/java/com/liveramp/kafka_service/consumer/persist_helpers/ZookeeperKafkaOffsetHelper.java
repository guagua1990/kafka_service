package com.liveramp.kafka_service.consumer.persist_helpers;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.kafka.common.TopicPartition;

import com.liveramp.kafka_service.zookeeper.ZookeeperClient;
import com.liveramp.kafka_service.zookeeper.ZookeeperEnv;

public class ZookeeperKafkaOffsetHelper implements KafkaOffsetHelper {

  private final ZookeeperClient zookeeperClient;

  private ZookeeperKafkaOffsetHelper(final EnumSet<ZookeeperEnv.ZKEnsembles> zkEnsembles) {
    this.zookeeperClient = new ZookeeperClient.Builder(zkEnsembles).build();
  }

  public static ZookeeperKafkaOffsetHelper create(final EnumSet<ZookeeperEnv.ZKEnsembles> zkEnsembles) {
    return new ZookeeperKafkaOffsetHelper(zkEnsembles);
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
