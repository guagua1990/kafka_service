<<<<<<< HEAD
//package com.liveramp.kafka_service.consumer;
//
//import java.io.FileNotFoundException;
//import java.io.FileReader;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//import java.util.Properties;
//import java.util.Set;
//
//import com.google.common.collect.Maps;
//import kafka.consumer.Consumer;
//import kafka.consumer.ConsumerConfig;
//import kafka.consumer.ConsumerIterator;
//import kafka.consumer.KafkaStream;
//import kafka.javaapi.consumer.ConsumerConnector;
//import org.json.JSONObject;
//import org.jvyaml.YAML;
//
//import com.liveramp.kafka_service.consumer.utils.JsonFactory;
//import com.liveramp.kafka_service.db_models.DatabasesImpl;
//import com.liveramp.kafka_service.db_models.db.IKafkaService;
//import com.liveramp.kafka_service.db_models.db.iface.IJobStatPersistence;
//import com.liveramp.kafka_service.db_models.db.models.JobStat;
//import com.rapleaf.support.Pair;
//
//public class StatsMerger extends Thread {
//
//  private final IKafkaService db;
//  private final IJobStatPersistence jobStatPersist;
//  private final ConsumerConnector consumerConnector;
//  private final Map<Long, Pair<Long, Long>> jobToTotalAndError;
//
//  public static void main(String[] args) throws FileNotFoundException {
//    StatsMerger logConsumer = new StatsMerger();
//    logConsumer.start();
//  }
//
//  public StatsMerger() throws FileNotFoundException {
//    db = new DatabasesImpl().getKafkaService();
//    db.disableCaching();
//    db.setAutoCommit(true);
//    jobStatPersist = db.jobStats();
//    consumerConnector = getConsumerConnector();
//    jobToTotalAndError = Maps.newHashMap();
//  }
//
//  @Override
//  public void run() {
//    ConsumerIterator<byte[], byte[]> it = getMsgIter();
//
//    try {
//      while (it.hasNext()) {
//        String statsEntry = new String(it.next().message());
//        JSONObject object = new JSONObject(statsEntry);
//        JsonFactory.StatsType statsType = JsonFactory.StatsType.valueOf(object.getString(JsonFactory.STATS_TYPE));
//        String statJsonString = object.getString(JsonFactory.STAT);
//
//        System.out.println(statJsonString);
//        updateDb(statsType, statJsonString);
//      }
//    } catch (Exception e) {
//      throw new RuntimeException(e);
//    }
//  }
//
//  private void updateDb(JsonFactory.StatsType statsType, String statJsonString) throws Exception {
//    if (statsType != JsonFactory.StatsType.TOTAL_COUNT && statsType != JsonFactory.StatsType.ERROR_COUNT) {
//      return;
//    }
//
//    JSONObject object = new JSONObject(statJsonString);
//    long jobId = object.getLong(JsonFactory.JOB_ID);
//    long count = object.getLong(JsonFactory.COUNT);
//
//    Set<JobStat> jobStats = jobStatPersist.query().jobId(jobId).find();
//    long timestamp = System.currentTimeMillis();
//
//    if (jobStats.isEmpty()) {
//      if (statsType == JsonFactory.StatsType.TOTAL_COUNT) {
//        jobToTotalAndError.put(jobId, Pair.makePair(count, 0L));
//        jobStatPersist.create(jobId, 0L, count, 0L, timestamp, timestamp);
//      } else {
//        jobToTotalAndError.put(jobId, Pair.makePair(0L, count));
//        jobStatPersist.create(jobId, count, 0L, 0L, timestamp, timestamp);
//      }
//    } else {
//      if (!jobToTotalAndError.containsKey(jobId)) {
//        jobToTotalAndError.put(jobId, Pair.makePair(0L, 0L));
//      }
//      Pair<Long, Long> pair = jobToTotalAndError.get(jobId);
//      if (statsType == JsonFactory.StatsType.TOTAL_COUNT) {
//        jobToTotalAndError.put(jobId, Pair.makePair(count + pair.first, pair.second));
//        getOneOrThrowException(jobStatPersist.query()
//            .jobId(jobId)
//            .find())
//            .setCountActualTotal(count + pair.first)
//            .setUpdatedAt(timestamp).save();
//      } else {
//        jobToTotalAndError.put(jobId, Pair.makePair(pair.first, pair.second + count));
//        getOneOrThrowException(jobStatPersist.query()
//            .jobId(jobId)
//            .find())
//            .setCountError(count + pair.second)
//            .setUpdatedAt(timestamp).save();
//      }
//    }
//  }
//
//  private JobStat getOneOrThrowException(Set<JobStat> jobStats) {
//    Iterator<JobStat> it = jobStats.iterator();
//    JobStat jobStat = it.next();
//    if (it.hasNext()) {
//      throw new RuntimeException("Job id is not uniq in the table.");
//    }
//    return jobStat;
//  }
//
//  private static ConsumerConnector getConsumerConnector() throws FileNotFoundException {
//    Properties properties = new Properties();
//    Map map = (Map)YAML.load(new FileReader("config/zookeeper-client.yaml"));
//    properties.put("zookeeper.connect", map.get("zookeeper.connect"));
//    properties.put("group.id", "test-par-1");
//    ConsumerConfig consumerConfig = new ConsumerConfig(properties);
//    return Consumer.createJavaConsumerConnector(consumerConfig);
//  }
//
//  private ConsumerIterator<byte[], byte[]> getMsgIter() {
//    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
//    topicCountMap.put(ConsumerConstants.MERGER_TOPIC, 1);
//    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
//    KafkaStream<byte[], byte[]> stream = consumerMap.get(ConsumerConstants.MERGER_TOPIC).get(0);
//    return stream.iterator();
//  }
//}
=======
package com.liveramp.kafka_service.consumer;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.google.common.collect.Maps;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.json.JSONObject;
import org.jvyaml.YAML;

import com.liveramp.kafka_service.consumer.utils.JsonFactory;
import com.liveramp.kafka_service.db_models.DatabasesImpl;
import com.liveramp.kafka_service.db_models.db.IKafkaService;
import com.liveramp.kafka_service.db_models.db.iface.IJobStatPersistence;
import com.liveramp.kafka_service.db_models.db.models.JobStat;
import com.rapleaf.support.Pair;

public class StatsMerger extends Thread {

  private final IKafkaService db;
  private final IJobStatPersistence jobStatPersist;
  private final ConsumerConnector consumerConnector;
  private final Map<Long, Pair<Long, Long>> jobToTotalAndError;

  public static void main(String[] args) throws FileNotFoundException {
    StatsMerger logConsumer = new StatsMerger();
    logConsumer.start();
  }

  public StatsMerger() throws FileNotFoundException {
    db = new DatabasesImpl().getKafkaService();
    db.disableCaching();
    db.setAutoCommit(true);
    jobStatPersist = db.jobStats();
    consumerConnector = getConsumerConnector();
    jobToTotalAndError = Maps.newHashMap();
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

    Set<JobStat> jobStats = jobStatPersist.query().jobId(jobId).find();
    long timestamp = System.currentTimeMillis();

    if (jobStats.isEmpty()) {
      if (statsType == JsonFactory.StatsType.TOTAL_COUNT) {
        jobToTotalAndError.put(jobId, Pair.makePair(count, 0L));
        jobStatPersist.create(jobId, 0L, count, 0L, timestamp, timestamp);
      } else {
        jobToTotalAndError.put(jobId, Pair.makePair(0L, count));
        jobStatPersist.create(jobId, count, 0L, 0L, timestamp, timestamp);
      }
    } else {
      if (!jobToTotalAndError.containsKey(jobId)) {
        jobToTotalAndError.put(jobId, Pair.makePair(0L, 0L));
      }
      Pair<Long, Long> pair = jobToTotalAndError.get(jobId);
      if (statsType == JsonFactory.StatsType.TOTAL_COUNT) {
        jobToTotalAndError.put(jobId, Pair.makePair(count + pair.first, pair.second));
        getOneOrThrowException(jobStatPersist.query()
            .jobId(jobId)
            .find())
            .setCountActualTotal(count + pair.first)
            .setUpdatedAt(timestamp).save();
      } else {
        jobToTotalAndError.put(jobId, Pair.makePair(pair.first, pair.second + count));
        getOneOrThrowException(jobStatPersist.query()
            .jobId(jobId)
            .find())
            .setCountError(count + pair.second)
            .setUpdatedAt(timestamp).save();
      }
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
    properties.put("group.id", "test-par-1");
    ConsumerConfig consumerConfig = new ConsumerConfig(properties);
    return Consumer.createJavaConsumerConnector(consumerConfig);
  }

  private ConsumerIterator<byte[], byte[]> getMsgIter() {
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(ConsumerConstants.MERGER_TOPIC, 1);
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
    KafkaStream<byte[], byte[]> stream = consumerMap.get(ConsumerConstants.MERGER_TOPIC).get(0);
    return stream.iterator();
  }
}
>>>>>>> e96a87cfd7cec410dd4bc9fbefd35a1f9dab09a3
