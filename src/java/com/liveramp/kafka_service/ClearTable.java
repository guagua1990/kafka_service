package com.liveramp.kafka_service;

import com.liveramp.kafka_service.db_models.DatabasesImpl;
import com.liveramp.kafka_service.db_models.db.iface.IJobStatPersistence;

public class ClearTable {
  private static final IJobStatPersistence jobStatPersist = new DatabasesImpl().getKafkaService().jobStats();

  public static void main(String[] args) throws Exception {
    jobStatPersist.deleteAll();
  }
}
