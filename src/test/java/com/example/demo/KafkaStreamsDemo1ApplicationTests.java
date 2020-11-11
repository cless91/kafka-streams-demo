package com.example.demo;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaStreamsDemo1ApplicationTests {

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, Long> inputTopic;
  private TestOutputTopic<String, Long> outputTopic;
  private KeyValueStore<String, Long> store;

  private Serde<String> stringSerde = new Serdes.StringSerde();
  private Serde<Long> longSerde = new Serdes.LongSerde();

  @BeforeEach
  void setUp() {
    Topology topology = new Topology();
    topology.addSource("sourceProcessor", "input-topic");
    topology.addProcessor("aggregator", CustomMaxAggregator::new, "sourceProcessor");
    topology.addStateStore(Stores.keyValueStoreBuilder(
        Stores.inMemoryKeyValueStore("aggStore"),
        Serdes.String(),
        Serdes.Long()).withLoggingDisabled(),
        "aggregator");
    topology.addSink("sinkProcessor", "result-topic", "aggregator");

    // setup test driver
    Properties properties = new Properties();
    properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "maxAggregation");
    properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());
    testDriver = new TopologyTestDriver(topology, properties);

    // setup test topics
    inputTopic = testDriver.createInputTopic("input-topic", stringSerde.serializer(), longSerde.serializer());
    outputTopic = testDriver.createOutputTopic("result-topic", stringSerde.deserializer(), longSerde.deserializer());

    // pre-populate store
    store = testDriver.getKeyValueStore("aggStore");
    store.put("a", 21L);
  }

  @AfterEach
  void tearDown() {
    testDriver.close();
  }

  @Test
  void shouldFlushStoreForFirstInput() {
    inputTopic.pipeInput("a", 1L);
    assertThat(store.get("a")).isEqualTo(21L);
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a", 21L));
    assertThat(outputTopic.isEmpty()).isTrue();
  }

  @Test
  void shouldUpdateStoreForLargerValue() {
    inputTopic.pipeInput("a", 42L);
    assertThat(store.get("a")).isEqualTo(42L);
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a", 42L));
    assertThat(outputTopic.isEmpty()).isTrue();
  }

  @Test
  void shouldUpdateStoreForNewKey() {
    inputTopic.pipeInput("b", 33L);
    assertThat(store.get("a")).isEqualTo(21L);
    assertThat(store.get("b")).isEqualTo(33L);
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a", 21L));
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("b", 33L));
    assertThat(outputTopic.isEmpty()).isTrue();
  }

  @Test
  void shouldPunctuateIfEventTimeAdvances() {
    final Instant recordTime = Instant.now();
    inputTopic.pipeInput("a", 1L, recordTime);
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a", 21L));

    inputTopic.pipeInput("a", 1L, recordTime);
    assertThat(outputTopic.isEmpty()).isTrue();

    inputTopic.pipeInput("a", 1L, recordTime.plusSeconds(11));
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a", 21L));
    assertThat(outputTopic.isEmpty()).isTrue();
  }

  @Test
  public void shouldPunctuateIfWallClockTimeAdvances() {
    assertThat(outputTopic.isEmpty()).isTrue();
    testDriver.advanceWallClockTime(Duration.ofSeconds(60));
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a", 21L));
    assertThat(outputTopic.isEmpty()).isTrue();
    testDriver.advanceWallClockTime(Duration.ofSeconds(60));
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a", 21L));
    assertThat(outputTopic.isEmpty()).isTrue();
    testDriver.advanceWallClockTime(Duration.ofSeconds(123));
    assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("a", 21L));
    assertThat(outputTopic.isEmpty()).isTrue();
  }

}

