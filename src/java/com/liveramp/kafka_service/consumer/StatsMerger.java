package com.liveramp.kafka_service.consumer;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
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
import com.liveramp.kafka_service.consumer.utils.StatsSummer;
import com.liveramp.kafka_service.db_models.DatabasesImpl;
import com.liveramp.kafka_service.db_models.db.iface.IJobStatPersistence;
import com.liveramp.kafka_service.db_models.db.models.JobStat;

public class StatsMerger extends Thread {

  private final IJobStatPersistence jobStatPersist;
  private final ConsumerConnector consumerConnector;
  private final StatsSummer statsSummer;

  public static void main(String[] args) throws FileNotFoundException {
    StatsMerger logConsumer = new StatsMerger();
    logConsumer.start();
  }

  public StatsMerger() throws FileNotFoundException {
    jobStatPersist = new DatabasesImpl().getKafkaService().jobStats();
    consumerConnector = getConsumerConnector();
    statsSummer = new StatsSummer();
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

        statsSummer.summStatJson(statsType, statJsonString);
        updateDb(statsSummer, statJsonString);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void updateDb(StatsSummer statsSummer, String statJsonString) throws Exception {
    JSONObject object = new JSONObject(statJsonString);
    long jobId = object.getLong(JsonFactory.JOB_ID);
    long ircId = object.getLong(JsonFactory.IRC_ID);
    long fieldId = object.getLong(JsonFactory.FIELD_ID);
    long totalCount = statsSummer.getTotalCount(jobId, ircId, fieldId);
    long errorCount = statsSummer.getErrorCount(jobId, ircId, fieldId);

    long timestamp = System.currentTimeMillis();

    Set<JobStat> jobStats = jobStatPersist.query()
        .jobId(jobId)
        .find();

    if (jobStats.isEmpty()) {
      jobStatPersist.create(jobId, errorCount, totalCount, 0L, timestamp, timestamp);
    } else {
      JobStat jobStat = jobStats.iterator().next();
      totalCount += jobStat.getCountActualTotal();
      errorCount += jobStat.getCountError();

      jobStat.setCountActualTotal(totalCount)
          .setCountError(errorCount)
          .setUpdatedAt(timestamp)
          .save();
    }
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
