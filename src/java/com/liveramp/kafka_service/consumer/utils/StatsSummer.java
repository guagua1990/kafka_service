package com.liveramp.kafka_service.consumer.utils;

import java.util.List;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.google.common.collect.Lists;
import org.json.JSONException;
import org.json.JSONObject;

import com.liveramp.commons.collections.nested_map.TwoKeyTuple;
import com.liveramp.commons.collections.nested_map.TwoNestedMap;
import com.liveramp.kafka_service.consumer.utils.JsonFactory;

public class StatsSummer {
  final static int HLL_PRECISION = 14;

  private static final TwoNestedMap<String, Long, Long> totalCountMap = new TwoNestedMap<String, Long, Long>();
  private static final TwoNestedMap<String, Long, Double> valueMap = new TwoNestedMap<String, Long, Double>();
  private static final TwoNestedMap<String, Long, HyperLogLogPlus> uniqueClickCountMap = new TwoNestedMap<String, Long, HyperLogLogPlus>();
  private static final TwoNestedMap<String, String, Long> errorCountMap = new TwoNestedMap<String, String, Long>();

  public void summJson(String jsonStr) throws JSONException {
    JSONObject json = new JSONObject(jsonStr);
    long jobId = json.getLong("job_id");
    long ircId = json.getLong("irc_id");
    int fieldId = json.getInt("field_definition_id");
    double value = json.getDouble("value");
    String clickUid = json.getString("click_uid");

    String key1 = getFirstKey(jobId, ircId);
    long key2 = (long)fieldId;


    if (!totalCountMap.containsKey(key1, key2)) {
      totalCountMap.put(key1, key2, 0L);
      valueMap.put(key1, key2, 0.0);
      uniqueClickCountMap.put(key1, key2, new HyperLogLogPlus(HLL_PRECISION));
    }
    totalCountMap.put(key1, key2, totalCountMap.get(key1, key2) + 1);
    valueMap.put(key1, key2, valueMap.get(key1, key2) + value);
    uniqueClickCountMap.get(key1, key2).offer(clickUid.getBytes());

    // count across all fields for job and irc.
    totalCountMap.put(key1, -1L, totalCountMap.containsKey(key1, -1L) ? totalCountMap.get(key1, -1L) + 1 : 1L);

    if (json.has("status")) {
      try {
        String k2 = getErrorSecondKey(json);
        if (!errorCountMap.containsKey(key1, k2)) {
          errorCountMap.put(key1, k2, 0L);
        }
        errorCountMap.put(key1, k2, errorCountMap.get(key1, k2) + 1);
      } catch (JSONException e) {
        throw new RuntimeException("Status and category enum id are inconsistent in the log");
      }
    }
  }

  public static String getFirstKey(long jobId, long ircId) {
    return jobId + "-" + ircId;
  }

  public static String getErrorSecondKey(JSONObject json) throws JSONException {
    long fieldId = json.getLong("field_definition_id");
    int categoryEnumId = json.getInt("category_enum_id");

    return fieldId + "-" + categoryEnumId;
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

  public void clear() {
    totalCountMap.clear();
    valueMap.clear();
    uniqueClickCountMap.clear();
    errorCountMap.clear();
  }
}
