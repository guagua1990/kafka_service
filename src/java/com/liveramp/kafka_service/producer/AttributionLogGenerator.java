package com.liveramp.kafka_service.producer;

import java.util.List;
import java.util.Random;

import com.google.common.collect.Lists;
import org.json.JSONException;
import org.json.JSONObject;

import com.rapleaf.spruce_lib.log.SpruceLogEntry;

public class AttributionLogGenerator {

  public static class AttributionLogBuilder extends SpruceLogEntry {

    public static final String GOOD_REQUEST_CATEGORY = "attribution_requests";
    public static final String BAD_REQUEST_CATEGORY = "attribution_bad_requests";

    private final JSONObject json;

    public AttributionLogBuilder(boolean isSuccess) throws JSONException {
      super(isSuccess ? GOOD_REQUEST_CATEGORY : BAD_REQUEST_CATEGORY);
      this.json = new JSONObject();
      json.put("ssa_id", 1111);
      json.put("upload_timestamp", 123123);
    }

    public AttributionLogBuilder setAmount(double amount) throws JSONException {
      json.put("value", amount);
      return this;
    }

    public AttributionLogBuilder setTimestamp(long timestamp) throws JSONException {
      json.put("transaction_timestamp", timestamp);
      return this;
    }

    public AttributionLogBuilder setDeviceId(String deviceId) throws JSONException {
      json.put("click_uid", deviceId);
      return this;
    }

    public AttributionLogBuilder setFieldId(int fieldId) throws JSONException {
      json.put("field_definition_id", fieldId);
      return this;
    }

    public AttributionLogBuilder setJobId(int jobId) throws JSONException {
      json.put("job_id", jobId);
      return this;
    }

    public AttributionLogBuilder setIrcId(int ircId) throws JSONException {
      json.put("irc_id", ircId);
      return this;
    }

    public AttributionLogBuilder setCategoryEnumId(int categoryEnumId) throws JSONException {
      json.put("category_enum_id", categoryEnumId);
      return this;
    }

    public AttributionLogBuilder setStatus(String status) throws JSONException {
      json.put("status", status);
      return this;
    }

    @Override
    public String toString() {
      return getTimeHostPrefix() + " attribution" + ": " + json.toString();
    }
  }

  public static List<AttributionLogBuilder> buildNLogs(int n) throws JSONException {
    List<AttributionLogBuilder> logs = Lists.newArrayList();

    for (int i = 0; i < n; i++) {
      Random random = new Random();
      logs.add(new AttributionLogBuilder(true)
              .setAmount(random.nextDouble())
              .setTimestamp(random.nextLong())
              .setDeviceId("device" + random.nextLong())
              .setFieldId(random.nextInt())
              .setJobId(random.nextInt())
              .setIrcId(random.nextInt())
              .setCategoryEnumId(random.nextInt())
              .setStatus("status" + random.nextInt())
      );
    }
    return logs;
  }

}
