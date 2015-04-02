
/**
 * Autogenerated by Jack
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 */
package com.liveramp.kafka_service.db_models;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

import org.jvyaml.YAML;

import com.rapleaf.jack.BaseDatabaseConnection;
import com.rapleaf.jack.DatabaseConnection;
import com.liveramp.kafka_service.db_models.db.IKafkaService;
import com.liveramp.kafka_service.db_models.db.impl.KafkaServiceImpl;

public class DatabasesImpl implements IDatabases {
  private final IKafkaService kafka_service;

  public DatabasesImpl(BaseDatabaseConnection kafka_service_connection) {
    this.kafka_service = new KafkaServiceImpl(kafka_service_connection, this);
  }

  public DatabasesImpl() {
    // load database info from config folder
    Map env_info;
    try {
      env_info  = (Map)YAML.load(new FileReader("config/environment.yml"));
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
      this.kafka_service = new KafkaServiceImpl(new DatabaseConnection("kafka_service"), this);

  }

  public IKafkaService getKafkaService() {
    return kafka_service;
  }
}
