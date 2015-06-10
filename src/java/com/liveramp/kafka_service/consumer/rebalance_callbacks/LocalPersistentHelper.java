package com.liveramp.kafka_service.consumer.rebalance_callbacks;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.kafka.common.TopicPartition;
import org.jvyaml.YAML;

public class LocalPersistentHelper implements PersistentHelper {

  private static final String PARTITION_KEY = "PK";

  private final String workingDirectory;

  private LocalPersistentHelper(final String workingDirectory) {
    this.workingDirectory = workingDirectory;
  }

  public static LocalPersistentHelper create(final String workingDirectory) {
    return new LocalPersistentHelper(workingDirectory);
  }


  @Override
  public boolean persistOffset(final TopicPartition partition, final long offset) {
    try {
      FileWriter fileWriter = new FileWriter(workingDirectory + partition.toString(), false);
      YAML.dump(Maps.newHashMap(), fileWriter);
      fileWriter.close();
    } catch (IOException e) {
      throw new RuntimeException(String.format("Failed to persist offset for %s: %s", partition.toString(), e.toString()));
    }
    return true;
  }

  @Override
  public boolean persistOffsets(final Map<TopicPartition, Long> topicPartitionOffsets) {
    for (Map.Entry<TopicPartition, Long> entry : topicPartitionOffsets.entrySet()) {
      if (!persistOffset(entry.getKey(), entry.getValue())) {
        return false;
      }
    }
    return true;
  }

  @Override
  public Long retrieveOffset(final TopicPartition partition) {
    try {
      FileReader fileReader = new FileReader(workingDirectory + partition.toString());
      Map<String, String> map = (Map<String, String>)YAML.load(fileReader);
      fileReader.close();
      return Long.valueOf(map.get(PARTITION_KEY));
    } catch (FileNotFoundException e) {
      throw new RuntimeException(String.format("No existing offset for %s: %s", partition.toString(), e.toString()));
    } catch (IOException e) {
      throw new RuntimeException(String.format("Failed to retrieve offset for %s: %s", partition.toString(), e.toString()));
    }
  }

  @Override
  public Map<TopicPartition, Long> retrieveOffsets(final Collection<TopicPartition> partitions) {
    Map<TopicPartition, Long> offsetsMap = Maps.newHashMap();

    for (TopicPartition partition : partitions) {
      offsetsMap.put(partition, retrieveOffset(partition));
    }

    return offsetsMap;
  }
}
