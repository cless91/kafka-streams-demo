package com.example.demo;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class PipeStreamsDslWithTestUtils {
  private TopologyTestDriver testDriver;
  private TestInputTopic<String, String> inputTopic;
  private TestOutputTopic<String, String> outputTopic;
  private Serde<String> stringSerde = new Serdes.StringSerde();

  @BeforeEach
  void setUp() {
    Properties properties = new Properties();
    properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "pipe");
    properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy");
    properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    StreamsBuilder builder = new StreamsBuilder();
    builder.stream("input-topic").to("result-topic");

    // setup test driver
    testDriver = new TopologyTestDriver(builder.build(), properties);

    // setup test topics
    inputTopic = testDriver.createInputTopic("input-topic", stringSerde.serializer(), stringSerde.serializer());
    outputTopic = testDriver.createOutputTopic("result-topic", stringSerde.deserializer(), stringSerde.deserializer());
  }

  @Test
  void shouldPipeInput() {
    assertThat(outputTopic.isEmpty()).isTrue();
    inputTopic.pipeInput("hello");
    assertThat(outputTopic.isEmpty()).isFalse();
    assertThat(outputTopic.readValue()).isEqualTo("hello");
    assertThat(outputTopic.isEmpty()).isTrue();
  }
}
