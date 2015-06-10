package com.liveramp.kafka_service.producer.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ProducerConfigFromFiles {

  public static Properties load(String path) throws IOException {
    Properties properties = new Properties();
    if (path.endsWith(".xml")) {
      properties.loadFromXML(new FileInputStream(path));
    } else {
      properties.load(new FileInputStream(path));
    }
    return properties;
  }

}
