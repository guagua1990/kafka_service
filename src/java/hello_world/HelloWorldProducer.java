package hello_world;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class HelloWorldProducer {
  final static String TOPIC = "hello_world";

  public static void main(String[] argv){
    Properties properties = new Properties();
    properties.put("metadata.broker.list","localhost:9092");
    properties.put("serializer.class","kafka.serializer.StringEncoder");
    ProducerConfig producerConfig = new ProducerConfig(properties);


    try {
      BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

      String input;

      while((input = br.readLine()) != null){
        kafka.javaapi.producer.Producer<String,String> producer = new kafka.javaapi.producer.Producer<String, String>(producerConfig);
        SimpleDateFormat sdf = new SimpleDateFormat();
        KeyedMessage<String, String> message =new KeyedMessage<String, String>(TOPIC, "syan: " + input + " " + sdf.format(new Date()));
        producer.send(message);
        producer.close();
      }

    } catch(IOException io){
      io.printStackTrace();
    }


  }
}
