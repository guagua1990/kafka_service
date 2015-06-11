package com.liveramp.kafka_service.consumer.rebalance_callbacks;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceCallback;
import org.apache.kafka.common.TopicPartition;

import com.liveramp.kafka_service.consumer.persist_helpers.KafkaOffsetHelper;

public class BaseRebalanceCallback implements ConsumerRebalanceCallback {

  private KafkaOffsetHelper kafkaOffsetHelper;

  private BaseRebalanceCallback(KafkaOffsetHelper kafkaOffsetHelper) {
    this.kafkaOffsetHelper = kafkaOffsetHelper;
  }

  public static BaseRebalanceCallback create(KafkaOffsetHelper kafkaOffsetHelper) {
    return new BaseRebalanceCallback(kafkaOffsetHelper);
  }

  @Override
  public void onPartitionsAssigned(final Consumer<?, ?> consumer, final Collection<TopicPartition> partitions) {
    Map<TopicPartition, Long> lastCommittedOffsets = kafkaOffsetHelper.retrieveOffsets(partitions);
    consumer.seek(lastCommittedOffsets);
  }

  @Override
  public void onPartitionsRevoked(final Consumer<?, ?> consumer, final Collection<TopicPartition> partitions) {
    // the consumer is responsible for committing the offsets
  }

}
