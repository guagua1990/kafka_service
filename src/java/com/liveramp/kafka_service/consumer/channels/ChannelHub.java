package com.liveramp.kafka_service.consumer.channels;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.liveramp.kafka_service.consumer.ConsumerConstants;
import com.liveramp.kafka_service.consumer.utils.StatsSummer;
import com.liveramp.kafka_service.producer.config.SyncProducerConfigBuilder;
import com.liveramp.kafka_service.producer.serializer.DefaultStringEncoder;

public class ChannelHub implements Runnable {
  private static final String clientChannel = "clientChannel";
  private static final String serverChannel = "serverChannel";
  private static final String channelType = "channelType";



  private final Producer<String, String> producer;
  private final StatsSummer statsSummer = new StatsSummer();


  public ChannelHub() {
    producer = getProducer();
  }

  private static Producer<String, String> getProducer() {
    ProducerConfig config = new SyncProducerConfigBuilder(new DefaultStringEncoder())
        .addBroker("s2s-data-syncer00", 9092)
        .addBroker("s2s-data-syncer01", 9092)
        .addBroker("s2s-data-syncer02", 9092)
        .addBroker("s2s-data-syncer03", 9092)
        .addBroker("s2s-data-syncer04", 9092)
        .build();

    return new Producer<String, String>(config);
  }

  /**
   * ServerSocketChannel represents a channel for sockets that listen to
   * incoming connections.
   *
   * @throws IOException
   */
  public void run() {
    try {
      int port = ConsumerConstants.PORT;
      String localhost = "localhost";

      // create a new serversocketchannel. The channel is unbound.
      ServerSocketChannel channel = ServerSocketChannel.open();

      // bind the channel to an address. The channel starts listening to
      // incoming connections.
      ServerSocket socket = channel.socket();
      socket.bind(new InetSocketAddress(localhost, port));

      // mark the serversocketchannel as non blocking
      channel.configureBlocking(false);

      // create a selector that will by used for multiplexing. The selector
      // registers the socketserverchannel as
      // well as all socketchannels that are created
      Selector selector = Selector.open();

      // register the serversocketchannel with the selector. The OP_ACCEPT
      // option marks
      // a selection key as ready when the channel accepts a new connection.
      // When the
      // socket server accepts a connection this key is added to the list of
      // selected keys of the selector.
      // when asked for the selected keys, this key is returned and hence we
      // know that a new connection has been accepted.
      SelectionKey socketServerSelectionKey = channel.register(selector,
          SelectionKey.OP_ACCEPT);
      // set property in the key that identifies the channel
      Map<String, String> properties = new HashMap<String, String>();
      properties.put(channelType, serverChannel);
      socketServerSelectionKey.attach(properties);
      // wait for the selected keys
      for (;;) {

        // the select method is a blocking method which returns when atleast
        // one of the registered
        // channel is selected. In this example, when the socket accepts a
        // new connection, this method
        // will return. Once a socketclient is added to the list of
        // registered channels, then this method
        // would also return when one of the clients has data to be read or
        // written. It is also possible to perform a nonblocking select
        // using the selectNow() function.
        // We can also specify the maximum time for which a select function
        // can be blocked using the select(long timeout) function.
        if (selector.select() == 0) {
          continue;
        }
        // the select method returns with a list of selected keys
        Set<SelectionKey> selectedKeys = selector.selectedKeys();
        Iterator<SelectionKey> iterator = selectedKeys.iterator();
        while (iterator.hasNext()) {
          SelectionKey key = iterator.next();
          // the selection key could either by the socketserver informing
          // that a new connection has been made, or
          // a socket client that is ready for read/write
          // we use the properties object attached to the channel to find
          // out the type of channel.
          if (((Map<?, ?>)key.attachment()).get(channelType).equals(serverChannel)) {
            // a new connection has been obtained. This channel is
            // therefore a socket server.
            ServerSocketChannel serverSocketChannel = (ServerSocketChannel)key.channel();
            // accept the new connection on the server socket. Since the
            // server socket channel is marked as non blocking
            // this channel will return null if no client is connected.
            SocketChannel clientSocketChannel = serverSocketChannel.accept();

            if (clientSocketChannel != null) {
              // set the client connection to be non blocking
              clientSocketChannel.configureBlocking(false);
              SelectionKey clientKey = clientSocketChannel.register(selector, SelectionKey.OP_READ);
              Map<String, String> clientproperties = new HashMap<String, String>();
              clientproperties.put(channelType, clientChannel);
              clientKey.attach(clientproperties);
            }

          } else {
            // data is available for read
            // buffer for reading
            ByteBuffer buffer = ByteBuffer.allocate(1000);
            SocketChannel clientChannel = (SocketChannel)key.channel();
            int bytesRead = 0;
            if (key.isReadable()) {
              StringBuilder messageBuilder = new StringBuilder();
              // the channel is non blocking so keep it open till the
              // count is >=0
              while ((bytesRead = clientChannel.read(buffer)) > 0) {
                buffer.flip();
                messageBuilder.append(Charset.defaultCharset().decode(buffer));
                buffer.clear();
              }
              String rawMsg = messageBuilder.toString();
              String[] messages = rawMsg.split(ConsumerConstants.MSG_DELIMITER);
              System.out.println(Arrays.toString(messages));

              // ** my logic starts **

              if (messages.length == 1 && messages[0].equals("!")) {
                List<String> strs = statsSummer.getStatsJsonStrings();
                System.out.println("sending " + strs.size() + " stats to stats-merge.");
                for (String str : strs) {
                  System.out.println(str);
                }
                sendStat(strs);
                statsSummer.clear();
              } else {
                for (String message : messages) {
                  statsSummer.summJson(message);
                }
              }

              // ** my logic ends **

              if (bytesRead < 0) {
                // the key is automatically invalidated once the
                // channel is closed
                clientChannel.close();
              }
            }

          }

          // once a key is handled, it needs to be removed
          iterator.remove();

        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  private void sendStat(List<String> jsonStats) {
    for (String jsonStat : jsonStats) {
      KeyedMessage<String, String> data = new KeyedMessage<String, String>(ConsumerConstants.MERGER_TOPIC, jsonStat);
      producer.send(data);
    }
  }

  public static void main(String[] args) {
    ChannelHub hub = new ChannelHub();
    hub.run();
  }
}