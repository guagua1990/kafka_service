package com.liveramp.kafka_service.consumer;


import org.json.JSONException;
import org.json.JSONObject;

public class JsonFactory {
  public enum StatsType {
    TOTAL_COUNT, TRANSACTION_VALUE, UNIQ_CLICK_COUNT, ERROR_COUNT
  }

  public static final String STATS_TYPE = "type";
  public static final String STAT = "stat";
  public static final String JOB_ID = "job_id";
  public static final String IRC_ID = "irc_id";
  public static final String FIELD_ID = "field_id";
  public static final String COUNT = "count";
  public static final String CATEGORY_ENUM_ID = "category_enum_id";

  public static String createTotalCountEntry(long jobId, long ircId, long fieldId, long count) throws JSONException {
    return createCountEntry(jobId, ircId, fieldId, count, StatsType.TOTAL_COUNT);
  }

  public static String createTransactionValueEntry(long jobId, long ircId, long fieldId, double value) throws JSONException {
    JSONObject json = new JSONObject();
    json.put(JOB_ID, jobId);
    json.put(IRC_ID, ircId);
    json.put(FIELD_ID, fieldId);
    json.put(COUNT, value);
    JSONObject wrapper = new JSONObject().put(STATS_TYPE, StatsType.TRANSACTION_VALUE.name());
    wrapper.put(STAT, json);
    return wrapper.toString();
  }

  public static String createUniqClickCountEntry(long jobId, long ircId, long fieldId, long count) throws JSONException {
    return createCountEntry(jobId, ircId, fieldId, count, StatsType.UNIQ_CLICK_COUNT);
  }

  public static String createErrorCountEntry(long jobId, long ircId, long fieldId, long categoryEnumId, long count) throws JSONException {
    JSONObject json = new JSONObject();
    json.put(JOB_ID, jobId);
    json.put(IRC_ID, ircId);
    json.put(FIELD_ID, fieldId);
    json.put(CATEGORY_ENUM_ID, categoryEnumId);
    json.put(COUNT, count);
    JSONObject wrapper = new JSONObject().put(STATS_TYPE, StatsType.ERROR_COUNT.name());
    wrapper.put(STAT, json);
    return wrapper.toString();
  }

  private static String createCountEntry(long jobId, long ircId, long fieldId, long count, StatsType type) throws JSONException {
    JSONObject json = new JSONObject();
    json.put(JOB_ID, jobId);
    json.put(IRC_ID, ircId);
    json.put(FIELD_ID, fieldId);
    json.put(COUNT, count);
    JSONObject wrapper = new JSONObject().put(STATS_TYPE, type.name());
    wrapper.put(STAT, json);
    return wrapper.toString();
  }

}
