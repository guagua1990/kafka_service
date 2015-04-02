package com.liveramp.kafka_service.consumer.channels;

public interface MessageIterator {

  public boolean hasNext();

  public String next();

}
