package com.liveramp.kafka_service.consumer;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.json.JSONObject;
import org.jvyaml.YAML;

import com.liveramp.kafka_service.consumer.utils.JsonFactory;
import com.liveramp.kafka_service.db_models.DatabasesImpl;
import com.liveramp.kafka_service.db_models.db.iface.IJobStatPersistence;
import com.liveramp.kafka_service.db_models.db.models.JobStat;

public class StatsMerger extends Thread {

  private final IJobStatPersistence jobStatPersist;
  private final ConsumerConnector consumerConnector;

  public static void main(String[] args) throws FileNotFoundException {
    StatsMerger logConsumer = new StatsMerger();
    logConsumer.start();
  }

  public StatsMerger() throws FileNotFoundException {
    jobStatPersist = new DatabasesImpl().getKafkaService().jobStats();
    consumerConnector = getConsumerConnector();
  }

  @Override
  public void run() {
    ConsumerIterator<byte[], byte[]> it = getMsgIter();

    try {
      while (it.hasNext()) {
        String statsEntry = new String(it.next().message());
        JSONObject object = new JSONObject(statsEntry);
        JsonFactory.StatsType statsType = JsonFactory.StatsType.valueOf(object.getString(JsonFactory.STATS_TYPE));
        String statJsonString = object.getString(JsonFactory.STAT);

        System.out.println(statJsonString);
        updateDb(statsType, statJsonString);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void updateDb(JsonFactory.StatsType statsType, String statJsonString) throws Exception {
    if (statsType != JsonFactory.StatsType.TOTAL_COUNT && statsType != JsonFactory.StatsType.ERROR_COUNT) {
      return;
    }

    JSONObject object = new JSONObject(statJsonString);
    long jobId = object.getLong(JsonFactory.JOB_ID);
    long count = object.getLong(JsonFactory.COUNT);

    Set<JobStat> jobStats = jobStatPersist.query()
        .jobId(jobId)
        .find();

    long timestamp = System.currentTimeMillis();

    if (jobStats.isEmpty()) {
      if (statsType == JsonFactory.StatsType.TOTAL_COUNT) {
        jobStatPersist.create(jobId, 0L, count, 0L, timestamp, timestamp);
      } else {
        jobStatPersist.create(jobId, count, 0L, 0L, timestamp, timestamp);
      }
    } else {
      JobStat jobStat = getOneOrThrowException(jobStats);

      if (statsType == JsonFactory.StatsType.TOTAL_COUNT) {
        count += jobStat.getCountActualTotal();
        jobStat.setCountActualTotal(count);
      } else {
        count += jobStat.getCountError();
        jobStat.setCountError(count);
      }

      jobStat.setUpdatedAt(timestamp).save();
    }
  }

  private JobStat getOneOrThrowException(Set<JobStat> jobStats) {
    Iterator<JobStat> it = jobStats.iterator();
    JobStat jobStat = it.next();
    if (it.hasNext()) {
      throw new RuntimeException("Job id is not uniq in the table.");
    }
    return jobStat;
  }

  private static ConsumerConnector getConsumerConnector() throws FileNotFoundException {
    Properties properties = new Properties();
    Map map = (Map)YAML.load(new FileReader("config/zookeeper-client.yaml"));
    properties.put("zookeeper.connect", map.get("zookeeper.connect"));
    properties.put("group.id","test-par-1");
    ConsumerConfig consumerConfig = new ConsumerConfig(properties);
    return Consumer.createJavaConsumerConnector(consumerConfig);
  }

  private ConsumerIterator<byte[], byte[]> getMsgIter() {
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(ConsumerConstants.MERGER_TOPIC, 1);
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
    KafkaStream<byte[], byte[]> stream =  consumerMap.get(ConsumerConstants.MERGER_TOPIC).get(0);
    return stream.iterator();
  }
}
