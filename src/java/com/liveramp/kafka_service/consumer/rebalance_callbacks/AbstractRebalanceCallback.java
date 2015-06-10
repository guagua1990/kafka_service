package com.liveramp.kafka_service.consumer.rebalance_callbacks;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;

abstract class AbstractRebalanceCallback implements LiverampRebalanceCallback {

  private PersistentHelper persistentHelper;

  AbstractRebalanceCallback(PersistentHelper persistentHelper) {
    this.persistentHelper = persistentHelper;
  }

  @Override
  public void onPartitionsAssigned(final Consumer<?, ?> consumer, final Collection<TopicPartition> partitions) {
    Map<TopicPartition, Long> lastCommittedOffsets = persistentHelper.retrieveOffsets(partitions);
    consumer.seek(lastCommittedOffsets);
  }

  @Override
  public void onPartitionsRevoked(final Consumer<?, ?> consumer, final Collection<TopicPartition> partitions) {
    // do nothing
  }

}
