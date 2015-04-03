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
import org.jvyaml.YAML;

import com.liveramp.kafka_service.db_models.DatabasesImpl;
import com.liveramp.kafka_service.db_models.db.IKafkaService;
import com.liveramp.kafka_service.db_models.db.iface.IJobStatPersistence;
import com.liveramp.kafka_service.db_models.db.models.JobStat;

public class TotalStatsConsumer extends Thread {

  private final IJobStatPersistence jobStatPersist;
  private final IKafkaService db;
  private final ConsumerConnector consumerConnector;

  public static void main(String[] args) throws FileNotFoundException {
    TotalStatsConsumer consumer = new TotalStatsConsumer();
    consumer.start();
  }

  public TotalStatsConsumer() throws FileNotFoundException {
    db = new DatabasesImpl().getKafkaService();
    db.disableCaching();
    jobStatPersist = db.jobStats();
    jobStatPersist.disableCaching();
    consumerConnector = getConsumerConnector();
  }

  @Override
  public void run() {
    ConsumerIterator<byte[], byte[]> it = getMsgIter();

    try {
      while (it.hasNext()) {
        String statsEntry = new String(it.next().message());

        String[] params = statsEntry.split(",");
        long jobId = Long.parseLong(params[0]);
        long chunkId = Long.parseLong(params[1]);
        long requestsNum = Long.parseLong(params[2]);

        System.out.println(statsEntry);
        updateDb(jobId, chunkId, requestsNum);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private synchronized void updateDb(long jobId, long chunkId, long requestNum) throws Exception {
    db.setAutoCommit(false);

    Set<JobStat> jobStats = jobStatPersist.query()
        .jobId(jobId)
        .find();

    long timestamp = System.currentTimeMillis();

    long expectedTotalCount = 0L;
    if (jobStats.isEmpty()) {
      expectedTotalCount = requestNum;
      jobStatPersist.create(jobId, 0L, 0L, requestNum, timestamp, timestamp);
    } else {
      JobStat jobStat = jobStats.iterator().next();
      expectedTotalCount = requestNum + jobStat.getCountExpectedTotal();

      boolean save = jobStat.setCountExpectedTotal(expectedTotalCount)
          .setUpdatedAt(timestamp)
          .save();
      if (!save) {
        System.out.println("!!!!!");
      }
    }

    db.commit();
    db.setAutoCommit(true);

    long queryAgain = jobStatPersist.query().jobId(jobId).find().iterator().next().getCountExpectedTotal();
    System.out.printf("count: %d, total: %d, readBack: %d\n", requestNum, expectedTotalCount, queryAgain);
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
    topicCountMap.put(ConsumerConstants.TOTAL_STATS_TOPIC, 1);
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
    KafkaStream<byte[], byte[]> stream =  consumerMap.get(ConsumerConstants.TOTAL_STATS_TOPIC).get(0);
    return stream.iterator();
  }
}
