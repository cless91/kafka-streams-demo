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

public class WordcountStreamsDslWithTestUtils {
  WordcountApplicationConfig applicationConfig = new WordcountApplicationConfig();
  Topology topology = applicationConfig.wordcountTopology();
  Properties properties = applicationConfig.wordcountProperties();

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, String> inputTopic;
  private TestOutputTopic<String, Long> outputTopic;
  private Serde<String> stringSerde = new Serdes.StringSerde();
  private Serde<Long> longSerde = new Serdes.LongSerde();

  @BeforeEach
  void setUp() {
    // setup test driver
    testDriver = new TopologyTestDriver(topology, properties);

    // setup test topics
    inputTopic = testDriver.createInputTopic(WordcountApplicationConfig.INPUT_TOPIC_NAME, stringSerde.serializer(), stringSerde.serializer());
    outputTopic = testDriver.createOutputTopic(WordcountApplicationConfig.OUTPUT_TOPIC_NAME, stringSerde.deserializer(), longSerde.deserializer());
  }

  @AfterEach
  void tearDown() {
    testDriver.close();
  }

  @Test
  void shouldCountWords() {
    assertThat(outputTopic.isEmpty()).isTrue();
    inputTopic.pipeInput("hello world");
    inputTopic.pipeInput("hello joseph");
    KeyValueStore<Object, Object> store = testDriver.getKeyValueStore(WordcountApplicationConfig.COUNT_STORE_NAME);
    assertThat(outputTopic.isEmpty()).isFalse();
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("hello", 1L));
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("world", 1L));
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("hello", 2L));
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("joseph", 1L));
    assertThat(outputTopic.isEmpty()).isTrue();
  }
}
