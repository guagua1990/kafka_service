package com.liveramp.kafka_service.producer.serializer;

import kafka.serializer.StringEncoder;
import kafka.utils.VerifiableProperties;

public class DefaultStringEncoder extends StringEncoder {

  public DefaultStringEncoder() {
    super(new VerifiableProperties());
  }

  public DefaultStringEncoder(VerifiableProperties props) {
    super(props);
  }
}
