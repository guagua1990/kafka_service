package com.liveramp.kafka_service.consumer.persist_helpers;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;

public interface PersistentHelper {

  public boolean persistOffset(final TopicPartition partition, final long offset);

  public boolean persistOffsets(final Map<TopicPartition, Long> topicPartitionOffsets);

  public Long retrieveOffset(final TopicPartition partition);

  public Map<TopicPartition, Long> retrieveOffsets(final Collection<TopicPartition> partitions);

}
