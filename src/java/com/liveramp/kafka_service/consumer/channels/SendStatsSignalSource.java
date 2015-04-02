package com.liveramp.kafka_service.consumer.channels;

import com.liveramp.kafka_service.consumer.ConsumerConstants;

public class SendStatsSignalSource implements MessageIterator {


  @Override
  public boolean hasNext() {
    try {
      Thread.sleep(ConsumerConstants.SEND_STATS_INTERVAL);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    return true;
  }

  @Override
  public String next() {
    return ConsumerConstants.SIGNAL_SYMBOL + ConsumerConstants.MSG_DELIMITER;
  }

  public static void main(String[] args) {
    MessageGenerator messageWorker = new MessageGenerator(new SendStatsSignalSource());
    messageWorker.run();
  }
}
