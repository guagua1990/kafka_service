package com.liveramp.kafka_service.zookeeper;

import java.io.IOException;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZookeeperClient extends ZooKeeper {
  private static final Logger LOG = LoggerFactory.getLogger(ZookeeperClient.class);


  public ZookeeperClient(String connectString, int sessionTimeout, Watcher watcher) throws IOException {
    super(connectString, sessionTimeout, watcher);
  }
}
