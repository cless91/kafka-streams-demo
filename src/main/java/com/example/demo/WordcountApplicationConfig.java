package com.example.demo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.Properties;

@Configuration
public class WordcountApplicationConfig {

  public static final String INPUT_TOPIC_NAME = "wordcount-input";
  public static final String OUTPUT_TOPIC_NAME = "wordcount-output";
  public static final String COUNT_STORE_NAME = "count-store";

  @Bean
  public Topology wordcountTopology() {
    StreamsBuilder builder = new StreamsBuilder();
    KTable<String, Long> count = builder.<String, String>stream(INPUT_TOPIC_NAME)
        .flatMapValues(line -> Arrays.asList(line.split("\\W+")))
        .groupBy((key, word) -> word)
        .count(Materialized.as(COUNT_STORE_NAME));

    count.toStream().to(OUTPUT_TOPIC_NAME, Produced.with(Serdes.String(),Serdes.Long() ));
    return builder.build();
  }

  @Bean
  public Properties wordcountProperties() {
    Properties properties = new Properties();
    properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "toUpperCase");
    properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    return properties;
  }
}
