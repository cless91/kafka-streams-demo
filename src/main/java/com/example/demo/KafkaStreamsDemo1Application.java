package com.example.demo;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Arrays;
import java.util.Properties;

@SpringBootApplication
public class KafkaStreamsDemo1Application implements ApplicationRunner {

	public static void main(String[] args) {
		SpringApplication.run(KafkaStreamsDemo1Application.class, args);
	}

	@Override
	public void run(ApplicationArguments args) throws Exception {
		Properties properties = new Properties();
		properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"wordcount-application");
		properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9094");
		properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());

		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> textlines = builder.stream("TextLinesTopic");
		KTable<String, Long> wordCount = textlines
				.flatMapValues(textline -> Arrays.asList(textline.split("\\W+")))
				.groupBy((key, word) -> word)
				.count(Materialized.as("count-store"));
		wordCount.toStream().to("WordsWithCountsTopic", Produced.with(Serdes.String(),Serdes.Long()));
		KafkaStreams streams = new KafkaStreams(builder.build(), properties);
		streams.start();
		Thread.sleep(200000000);
	}
}
