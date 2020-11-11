package com.example.demo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.Properties;

@Configuration
public class SplitLinesApplicationConfig {
  @Bean
  public Topology splitLinesTopology() {
    StreamsBuilder builder = new StreamsBuilder();
    builder.<String, String>stream("input-topic")
        .flatMapValues(value -> Arrays.asList(value.split("\\W+")))
        .to("result-topic");
    return builder.build();
  }

  @Bean
  public Properties splitLinesProperties() {
    Properties properties = new Properties();
    properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "toUpperCase");
    properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    return properties;
  }
}
