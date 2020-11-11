package com.example.demo;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class UppercaseStreamsDslWithTestUtils {
  ToUpperCaseApplicationConfig applicationConfig = new ToUpperCaseApplicationConfig();
  Topology topology = applicationConfig.toUpperCaseTopology();
  Properties properties = applicationConfig.toUpperCaseProperties();

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, String> inputTopic;
  private TestOutputTopic<String, String> outputTopic;
  private Serde<String> stringSerde = new Serdes.StringSerde();

  @BeforeEach
  void setUp() {
    // setup test driver
    testDriver = new TopologyTestDriver(topology, properties);

    // setup test topics
    inputTopic = testDriver.createInputTopic("input-topic", stringSerde.serializer(), stringSerde.serializer());
    outputTopic = testDriver.createOutputTopic("result-topic", stringSerde.deserializer(), stringSerde.deserializer());
  }

  @Test
  void shouldPipeInput() {
    assertThat(outputTopic.isEmpty()).isTrue();
    inputTopic.pipeInput("hello");
    assertThat(outputTopic.isEmpty()).isFalse();
    assertThat(outputTopic.readValue()).isEqualTo("HELLO");
    assertThat(outputTopic.isEmpty()).isTrue();
  }
}
