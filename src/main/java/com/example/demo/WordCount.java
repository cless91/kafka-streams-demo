package com.example.demo;

import io.vavr.Tuple;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class WordCount {
  public static void main(String[] args) {
    Properties properties = new Properties();
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount");
    properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
    properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

    StreamsBuilder builder = new StreamsBuilder();
    KTable<String, Long> count = builder.<String, String>stream("wordcount-input")
        .flatMapValues(line -> Arrays.asList(line.split("\\W+")))
        .groupBy((key, word) -> word)
        .count();

    count.toStream().to("wordcount-output", Produced.with(Serdes.String(),Serdes.Long() ));
    Topology topology = builder.build();

    KafkaStreams kafkaStreams = new KafkaStreams(topology, properties);
    doStart(kafkaStreams);
  }

  private static void doStart(KafkaStreams streams){
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
