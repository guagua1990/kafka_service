//package com.liveramp.kafka_service.consumer;
//
//import java.io.FileNotFoundException;
//import java.io.FileReader;
//import java.util.HashMap;
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
//import org.jvyaml.YAML;
//
//import com.liveramp.kafka_service.db_models.DatabasesImpl;
//import com.liveramp.kafka_service.db_models.db.IKafkaService;
//import com.liveramp.kafka_service.db_models.db.iface.IJobStatPersistence;
//import com.liveramp.kafka_service.db_models.db.models.JobStat;
//import com.rapleaf.support.collections.Accessors;
//
//public class TotalStatsConsumer extends Thread {
//
//  private final IJobStatPersistence jobStatPersist;
//  private final IKafkaService db;
//  private final ConsumerConnector consumerConnector;
//  private final Map<Long, Long> jobCount;
//
//  public static void main(String[] args) throws FileNotFoundException {
//    TotalStatsConsumer consumer = new TotalStatsConsumer();
//    consumer.start();
//  }
//
//  public TotalStatsConsumer() throws FileNotFoundException {
//    db = new DatabasesImpl().getKafkaService();
//    db.disableCaching();
//    db.setAutoCommit(true);
//    jobStatPersist = db.jobStats();
//    jobStatPersist.disableCaching();
//    consumerConnector = getConsumerConnector();
//    jobCount = Maps.newHashMap();
//  }
//
//  @Override
//  public void run() {
//    ConsumerIterator<byte[], byte[]> it = getMsgIter();
//
//    try {
//      while (it.hasNext()) {
//        String statsEntry = new String(it.next().message());
//
//        String[] params = statsEntry.split(",");
//        long jobId = Long.parseLong(params[0]);
//        long chunkId = Long.parseLong(params[1]);
//        long requestsNum = Long.parseLong(params[2]);
//
//        System.out.println(statsEntry);
//        updateDb(jobId, chunkId, requestsNum);
//      }
//    } catch (Exception e) {
//      throw new RuntimeException(e);
//    }
//  }
//
//  private void updateDb(long jobId, long chunkId, long requestNum) throws Exception {
//    long timestamp = System.currentTimeMillis();
//
//    Set<JobStat> jobStats = jobStatPersist.query().jobId(jobId).find();
//    if (jobStats.isEmpty()) {
//      jobCount.put(jobId, requestNum);
//      jobStatPersist.create(jobId, 0L, 0L, requestNum, timestamp, timestamp);
//    } else {
//      if (!jobCount.containsKey(jobId)) {
//        jobCount.put(jobId, 0L);
//      }
//      jobCount.put(jobId, jobCount.get(jobId) + requestNum);
//      Accessors.first(jobStatPersist.query().jobId(jobId).find())
//          .setCountExpectedTotal(jobCount.get(jobId))
//          .setUpdatedAt(timestamp)
//          .save();
//    }
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
//    topicCountMap.put(ConsumerConstants.TOTAL_STATS_TOPIC, 1);
//    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
//    KafkaStream<byte[], byte[]> stream = consumerMap.get(ConsumerConstants.TOTAL_STATS_TOPIC).get(0);
//    return stream.iterator();
//  }
//}
