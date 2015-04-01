package com.liveramp.kafka_service.consumer.config;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import org.jvyaml.YAML;

public class YamlConsumerConfigBuilder {

  public static ConsumerConfig buildFromYaml(String yamlPath) {
    try {
      Map map = (Map)YAML.load(new FileReader(yamlPath));
      Properties properties = new Properties();

      properties.put("zookeeper.connect", map.get("zookeeper"));
      properties.put("group.id", map.get("group"));
      properties.put("zookeeper.session.timeout.ms", "400");
      properties.put("zookeeper.sync.time.ms", "200");
      properties.put("auto.commit.interval.ms", "1000");

      return new ConsumerConfig(properties);
    } catch (FileNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
