package com.example.demo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.Properties;

@Configuration
public class JoinApplicationConfig {

  public static final String INPUT_A_TOPIC_NAME = "join-in-a";
  public static final String INPUT_B_TOPIC_NAME = "join-in-b";
  public static final String OUTPUT_TOPIC_NAME = "join-out";

  @Bean
  public Topology joinTopology() {
    StreamsBuilder builder = new StreamsBuilder();

    KStream<String, String> topinInAStream = builder.stream(INPUT_A_TOPIC_NAME);
    KStream<String, String> topinInBStream = builder.stream(INPUT_B_TOPIC_NAME);

    KStream<String, String> joinAB = topinInAStream
        .join(topinInBStream.toTable(), (value1, value2) -> value1 + "::" + value2);
    KStream<String, String> joinBA = topinInBStream
        .join(topinInAStream.toTable(), (value1, value2) -> value1 + "::" + value2);

    joinAB
        .merge(joinBA)
        .to(OUTPUT_TOPIC_NAME);
    return builder.build();
  }

  @Bean
  public Properties joinProperties() {
    Properties properties = new Properties();
    properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "toUpperCase");
    properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    return properties;
  }
}
