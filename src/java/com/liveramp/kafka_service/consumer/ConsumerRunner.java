package com.liveramp.kafka_service.consumer;

import java.io.FileNotFoundException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.liveramp.kafka_service.consumer.channels.ChannelHub;
import com.liveramp.kafka_service.consumer.channels.LogSource;
import com.liveramp.kafka_service.consumer.channels.MessageGenerator;
import com.liveramp.kafka_service.consumer.channels.SendStatsSignalSource;

public class ConsumerRunner {
  public static void main(String[] args) throws FileNotFoundException {
    MessageGenerator logConsumerThread = new MessageGenerator(new LogSource());
    MessageGenerator sendStatsSignalThread = new MessageGenerator(new SendStatsSignalSource());

    ChannelHub channelHub = new ChannelHub();

    channelHub.run();
    logConsumerThread.run();
    sendStatsSignalThread.run();
  }
}
