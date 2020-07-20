package com.mht.kafka.streams;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

public class FilterMessageStream {

	private static final String inputTopic = "kstream-input-topic";
	private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
	private static String keyword = "mohit";
	private static final String outputTopic = "kstream-output-topic";
	
	public static void main(String[] args) {
		// Create Properties
		Properties properties = new Properties();
		properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
		properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafk-stream"); // similar to consumer group in consumer
		properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
		properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
		
		//Create toptology
		StreamsBuilder streamsBuilder = new StreamsBuilder();
		
		//Input topic
		KStream<String, String> inputTopicStream = streamsBuilder.stream(inputTopic);
		KStream<String, String> filteredStream = inputTopicStream.filter(
				(key, message) -> {
					System.out.println(key + " "+ message);
					return message.contains(keyword);
				}
					//filter records containing the keyword
				);
		
		//Output topic
		filteredStream.to(outputTopic);
		
		//Build the topology
		KafkaStreams kafkaStreams = new KafkaStreams(
				streamsBuilder.build(), 
				properties
		);
				
		//Start our stream application
		kafkaStreams.start(); 
	}

}
