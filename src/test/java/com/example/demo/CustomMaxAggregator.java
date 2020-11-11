package com.example.demo;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;

public class CustomMaxAggregator implements Processor<String, Long> {
  ProcessorContext context;
  private KeyValueStore<String, Long> store;

  @SuppressWarnings("unchecked")
  @Override
  public void init(ProcessorContext context) {
    this.context = context;
    context.schedule(Duration.ofSeconds(60), PunctuationType.WALL_CLOCK_TIME, time -> flushStore());
    context.schedule(Duration.ofSeconds(10), PunctuationType.STREAM_TIME, time -> flushStore());
    store = (KeyValueStore<String, Long>) context.getStateStore("aggStore");
  }

  @Override
  public void process(String key, Long value) {
    Long oldValue = store.get(key);
    if (oldValue == null || value > oldValue) {
      store.put(key, value);
    }
  }

  private void flushStore() {
    KeyValueIterator<String, Long> it = store.all();
    while (it.hasNext()) {
      KeyValue<String, Long> next = it.next();
      context.forward(next.key, next.value);
    }
  }

  @Override
  public void close() {
  }
}
