package com.liveramp.kafka_service.consumer;

public class ConsumerConstants {
  public final static String MERGER_TOPIC = "stats_merge";
  public final static String TOTAL_STATS_TOPIC = "total_requests_per_chunk";
  public final static String DELIMITER = "attribution: ";
  public final static String CONSUMER_GROUP = "test-par-1";
  public final static long SEND_STATS_INTERVAL = 10 * 1000;
  public final static String SIGNAL_SYMBOL = "!";
  public final static String MSG_DELIMITER = "#";
}
