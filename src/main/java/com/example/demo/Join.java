package com.example.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Join {
  public static void main(String[] args) throws Exception {
    Properties properties = new Properties();
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "join");
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    StreamsBuilder builder = new StreamsBuilder();
//    String topicBaseName = UUID.randomUUID().toString();
    String topicBaseName = "join";
    String topicInA = topicBaseName + "-in-a";
    String topicInB = topicBaseName + "-in-b";

    KStream<String, String> topinInAStream = builder.stream(topicInA);
    KStream<String, String> topinInBStream = builder.stream(topicInB);

    KStream<String, String> joinAB = topinInAStream
        .join(topinInBStream.toTable(), (value1, value2) -> value1 + "::" + value2);
    KStream<String, String> joinBA = topinInBStream
        .join(topinInAStream.toTable(), (value1, value2) -> value1 + "::" + value2);

    joinAB
        .merge(joinBA)
        .to(topicBaseName + "-out");

    KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties);
    System.out.println("topicBaseName: " + topicBaseName);
    kafkaStreams.start();
    doSend(topicInA, topicInB);
    Thread.sleep(999999999);
  }

  private static void doSend(String topicInA, String topicInB) throws InterruptedException {
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig);
    Thread.sleep(2000);
    producer.send(new ProducerRecord<>(topicInA, "key", "A1"));
    producer.send(new ProducerRecord<>(topicInB, "key", "B4"));
    Thread.sleep(2000);
    producer.send(new ProducerRecord<>(topicInA, "key", "A2"));
    Thread.sleep(2000);
    producer.send(new ProducerRecord<>(topicInA, "key", "A3"));
    producer.send(new ProducerRecord<>(topicInB, "key", "B6"));
    Thread.sleep(2000);
    producer.send(new ProducerRecord<>(topicInA, "key", "A1"));
    Thread.sleep(2000);
    producer.send(new ProducerRecord<>(topicInA, "key", "A2"));
    Thread.sleep(2000);
    producer.send(new ProducerRecord<>(topicInA, "key", "A3"));
    producer.send(new ProducerRecord<>(topicInB, "key", "B9"));
    Thread.sleep(2000);
    producer.send(new ProducerRecord<>(topicInA, "key", null));
    producer.send(new ProducerRecord<>(topicInB, "key", null));
  }

  private static void doStart(KafkaStreams streams) {
    final CountDownLatch latch = new CountDownLatch(1);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      streams.close();
      latch.countDown();
    }));

    streams.start();
    try {
      latch.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
