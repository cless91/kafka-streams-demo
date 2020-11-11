package com.example.demo;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

public class JoinStreamsDslWithTestUtils {
  JoinApplicationConfig applicationConfig = new JoinApplicationConfig();
  Topology topology = applicationConfig.joinTopology();
  Properties properties = applicationConfig.joinProperties();

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, String> inputTopicA;
  private TestInputTopic<String, String> inputTopicB;
  private TestOutputTopic<String, String> outputTopic;
  private Serde<String> stringSerde = new Serdes.StringSerde();
  private Serde<Long> longSerde = new Serdes.LongSerde();

  @BeforeEach
  void setUp() {
    // setup test driver
    testDriver = new TopologyTestDriver(topology, properties);

    // setup test topics
    inputTopicA = testDriver.createInputTopic(JoinApplicationConfig.INPUT_A_TOPIC_NAME, stringSerde.serializer(), stringSerde.serializer());
    inputTopicB = testDriver.createInputTopic(JoinApplicationConfig.INPUT_B_TOPIC_NAME, stringSerde.serializer(), stringSerde.serializer());
    outputTopic = testDriver.createOutputTopic(JoinApplicationConfig.OUTPUT_TOPIC_NAME, stringSerde.deserializer(), stringSerde.deserializer());
  }

  @AfterEach
  void tearDown() {
    testDriver.close();
  }

  @Test
  void shouldJoin() {
    assertThat(outputTopic.isEmpty()).isTrue();
    inputTopicA.pipeInput("key","A1");
    inputTopicB.pipeInput("key","B1");
    assertThat(outputTopic.isEmpty()).isFalse();
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("key","B1::A1"));
  }
}
