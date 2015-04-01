package com.liveramp.kafka_service.producer.config;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;
import java.util.Properties;

import kafka.producer.ProducerConfig;
import org.jvyaml.YAML;

public class YamlProducerConfigBuilder {

  public static ProducerConfig buildFromYaml(String yamlPath) {
    try {
      Map map = (Map)YAML.load(new FileReader(yamlPath));
      Properties properties = new Properties();

      properties.put("producer.type", map.get("type"));
      properties.put("metadata.broker.list", map.get("broker.list"));
      properties.put("serializer.class", map.get("serializer"));
      properties.put("request.required.acks", "1");

      return new ProducerConfig(properties);
    } catch (FileNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
  }

}
