package com.liveramp.kafka_service.consumer.channels;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.CharBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.liveramp.kafka_service.consumer.ConsumerConstants;

public class MessageGenerator implements Runnable {

  private MessageIterator messageIterator;

  public MessageGenerator(MessageIterator messageIterator) {
    this.messageIterator = messageIterator;
  }

  @Override
  public void run() {
    int port = ConsumerConstants.PORT;
    SocketChannel channel = null;
    try {
      channel = SocketChannel.open();

      // we open this channel in non blocking mode
      channel.configureBlocking(false);
      channel.connect(new InetSocketAddress("localhost", port));

      while (!channel.finishConnect()) {
        System.out.println("still connecting...");
      }
      while (messageIterator.hasNext()) {
        CharBuffer buffer = CharBuffer.wrap(messageIterator.next());
        while (buffer.hasRemaining()) {
          channel.write(Charset.defaultCharset().encode(buffer));
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    ExecutorService executorService = Executors.newFixedThreadPool(2);

//    MessageInterface clientChannel1 = new MessageInterface("Hello, from client1,********************************");
//    MessageInterface clientChannel2 = new MessageInterface("!");
//
//    executorService.submit(clientChannel1);
//    executorService.submit(clientChannel2);
  }
}
