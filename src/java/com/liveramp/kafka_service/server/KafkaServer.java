package com.liveramp.kafka_service.server;

import org.I0Itec.zkclient.ZkClient;

public class KafkaServer {

  private ZkClient zkClient;

  public KafkaServer(String server) {
    this.zkClient = new ZkClient(server);
  }

  public static KafkaServer create(String server) {
    return new KafkaServer(server);
  }
}
