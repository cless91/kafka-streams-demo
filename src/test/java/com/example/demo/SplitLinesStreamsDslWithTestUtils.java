package com.example.demo;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class SplitLinesStreamsDslWithTestUtils {
  SplitLinesApplicationConfig applicationConfig = new SplitLinesApplicationConfig();
  Topology topology = applicationConfig.splitLinesTopology();
  Properties properties = applicationConfig.splitLinesProperties();

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
    inputTopic.pipeInput("hello. world");
    assertThat(outputTopic.isEmpty()).isFalse();
    assertThat(outputTopic.readValue()).isEqualTo("hello");
    assertThat(outputTopic.readValue()).isEqualTo("world");
    assertThat(outputTopic.isEmpty()).isTrue();
  }
}
