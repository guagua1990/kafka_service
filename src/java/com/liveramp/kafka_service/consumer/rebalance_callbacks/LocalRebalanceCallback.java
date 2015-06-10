package com.liveramp.kafka_service.consumer.rebalance_callbacks;

public class LocalRebalanceCallback extends AbstractRebalanceCallback {

  private LocalRebalanceCallback(final String workingDirectory) {
    super(LocalPersistentHelper.create(workingDirectory));
  }

  public static LocalRebalanceCallback create(final String workflowDirectory) {
    return new LocalRebalanceCallback(workflowDirectory);
  }

}
