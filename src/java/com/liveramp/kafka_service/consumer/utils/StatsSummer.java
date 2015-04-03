package com.liveramp.kafka_service.consumer.utils;

import java.util.List;
import java.util.Map;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.google.common.collect.Lists;
import kafka.utils.Json;
import org.json.JSONException;
import org.json.JSONObject;

import com.liveramp.commons.collections.nested_map.TwoKeyTuple;
import com.liveramp.commons.collections.nested_map.TwoNestedMap;

public class StatsSummer {
  final static int HLL_PRECISION = 14;

  private final TwoNestedMap<String, Long, Long> totalCountMap = new TwoNestedMap<String, Long, Long>();
  private final TwoNestedMap<String, Long, Double> valueMap = new TwoNestedMap<String, Long, Double>();
  private final TwoNestedMap<String, Long, HyperLogLogPlus> uniqueClickCountMap = new TwoNestedMap<String, Long, HyperLogLogPlus>();
  private final TwoNestedMap<String, String, Long> errorCountMap = new TwoNestedMap<String, String, Long>();

  public void summJson(String jsonStr) throws JSONException {
    JSONObject json = new JSONObject(jsonStr);
    long jobId = json.getLong("job_id");
    long ircId = json.getLong("irc_id");
    int fieldId = json.getInt("field_definition_id");
    double value = json.getDouble("value");
    String clickUid = json.getString("click_uid");

    String key1 = getCombinedKey(jobId, ircId);
    long key2 = (long)fieldId;

    if (!totalCountMap.containsKey(key1, key2)) {
      totalCountMap.put(key1, key2, 0L);
      valueMap.put(key1, key2, 0.0);
      uniqueClickCountMap.put(key1, key2, new HyperLogLogPlus(HLL_PRECISION));
    }
    totalCountMap.put(key1, key2, totalCountMap.get(key1, key2) + 1);
    valueMap.put(key1, key2, valueMap.get(key1, key2) + value);
    uniqueClickCountMap.get(key1, key2).offer(clickUid.getBytes());

    if (json.has("status")) {
      try {
        int categoryEnumId = json.getInt("category_enum_id");
        String k2 = getCombinedKey(fieldId, categoryEnumId);
        if (!errorCountMap.containsKey(key1, k2)) {
          errorCountMap.put(key1, k2, 0L);
        }
        errorCountMap.put(key1, k2, errorCountMap.get(key1, k2) + 1);
      } catch (JSONException e) {
        throw new RuntimeException("Status and category enum id are inconsistent in the log");
      }
    }
  }

  public static String getCombinedKey(long jobId, long ircId) {
    return jobId + "-" + ircId;
  }

  public TwoKeyTuple<Long, Long> separateTwoKeys(String str) {
    String[] keys = str.split("-");
    return new TwoKeyTuple<Long, Long>(Long.parseLong(keys[0]), Long.parseLong(keys[1]));
  }

  public List<String> getStatsJsonStrings() {
    List<String> statJsonStrings = Lists.newArrayList();

    for (TwoKeyTuple<String, Long> keys : totalCountMap.key12Set()) {
      TwoKeyTuple<Long, Long> twoKey = separateTwoKeys(keys.getK1());
      statJsonStrings.add(JsonFactory.createTotalCountEntry(twoKey.getK1(), twoKey.getK2(), keys.getK2(), totalCountMap.get(keys)));
    }

    for (TwoKeyTuple<String, Long> keys : valueMap.key12Set()) {
      TwoKeyTuple<Long, Long> twoKey = separateTwoKeys(keys.getK1());
      statJsonStrings.add(JsonFactory.createTransactionValueEntry(twoKey.getK1(), twoKey.getK2(), keys.getK2(), valueMap.get(keys)));
    }

    for (TwoKeyTuple<String, Long> keys : uniqueClickCountMap.key12Set()) {
      TwoKeyTuple<Long, Long> twoKey = separateTwoKeys(keys.getK1());
      statJsonStrings.add(JsonFactory.createUniqClickCountEntry(twoKey.getK1(), twoKey.getK2(), keys.getK2(), uniqueClickCountMap.get(keys).cardinality()));
    }

    for (TwoKeyTuple<String, String> keys : errorCountMap.key12Set()) {
      TwoKeyTuple<Long, Long> twoKey1 = separateTwoKeys(keys.getK1());
      TwoKeyTuple<Long, Long> twoKey2 = separateTwoKeys(keys.getK2());
      statJsonStrings.add(JsonFactory.createErrorCountEntry(twoKey1.getK1(), twoKey1.getK2(), twoKey2.getK1(), twoKey2.getK2(), errorCountMap.get(keys)));
    }
    return statJsonStrings;
  }

  public void summStatJson(JsonFactory.StatsType type, String statsJsonString) throws JSONException {
    JSONObject json = new JSONObject(statsJsonString);
    switch (type) {
      case TOTAL_COUNT:
        long count = totalCountMap
            .get(getCombinedKey(json.getLong(JsonFactory.JOB_ID), json.getLong(JsonFactory.IRC_ID)),
                json.getLong(JsonFactory.FIELD_ID));
        totalCountMap.put(getCombinedKey(json.getLong(JsonFactory.JOB_ID), json.getLong(JsonFactory.IRC_ID)),
            json.getLong(JsonFactory.FIELD_ID), count + json.getLong(JsonFactory.COUNT));
      case TRANSACTION_VALUE:
        double value = valueMap
            .get(getCombinedKey(json.getLong(JsonFactory.JOB_ID), json.getLong(JsonFactory.IRC_ID)),
                json.getLong(JsonFactory.FIELD_ID));
        valueMap
            .put(getCombinedKey(json.getLong(JsonFactory.JOB_ID), json.getLong(JsonFactory.IRC_ID)),
                json.getLong(JsonFactory.FIELD_ID), value + json.getDouble(JsonFactory.COUNT));
      case ERROR_COUNT:
        long count1 = errorCountMap
            .get(getCombinedKey(json.getLong(JsonFactory.JOB_ID), json.getLong(JsonFactory.IRC_ID)),
                getCombinedKey(json.getLong(JsonFactory.FIELD_ID), json.getLong(JsonFactory.CATEGORY_ENUM_ID)));
        errorCountMap
            .put(getCombinedKey(json.getLong(JsonFactory.JOB_ID), json.getLong(JsonFactory.IRC_ID)),
                getCombinedKey(json.getLong(JsonFactory.FIELD_ID), json.getLong(JsonFactory.CATEGORY_ENUM_ID)), count1 + json.getLong(JsonFactory.COUNT));
      default:
        break;
    }
  }

  public void clear() {
    totalCountMap.clear();
    valueMap.clear();
    uniqueClickCountMap.clear();
    errorCountMap.clear();
  }

  public long getTotalCount(long jobId, long ircId, long fieldId) {
    Long count = totalCountMap.get(getCombinedKey(jobId, ircId), fieldId);
    return count == null ? 0 : count;
  }

  public long getErrorCount(long jobId, long ircId, long fieldId) {
    Map<String, Long> subMap = errorCountMap.get(getCombinedKey(jobId, ircId));

    long count = 0;
    for (String combinedKey : subMap.keySet()) {
      TwoKeyTuple<Long, Long> key2 = separateTwoKeys(combinedKey);
      if (key2.getK1() == fieldId) {
        count += subMap.get(combinedKey);
      }
    }
    return count;
  }
}
