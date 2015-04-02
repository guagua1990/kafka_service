package com.liveramp.kafka_service.db_models.db.models;

import org.junit.Test;

import com.liveramp.kafka_service.db_models.IDatabases;
import com.liveramp.kafka_service.db_models.db.iface.IJobStatPersistence;
import com.liveramp.kafka_service.db_models.DatabasesImpl;
import com.rapleaf.jack.DatabaseConnection;

public class TestJobStat {
  private static final DatabaseConnection CONN = new DatabaseConnection("database");
  private static final IDatabases dbs = new DatabasesImpl(CONN);

  private final IJobStatPersistence jobStats = dbs.getKafkaService().jobStats();

  @Test
  public void testJobStatCreation() throws Exception {
    jobStats.createDefaultInstance()
        .save();

    jobStats.createDefaultInstance()
        .save();
  }
}
