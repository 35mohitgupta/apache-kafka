package org.mht.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerSeekAndAssign {

	private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
	private static final String topic = "demo_topic";
	private static Logger logger = LoggerFactory.getLogger(ConsumerSeekAndAssign.class);
	
	public static void main(String[] args) {
		
		//creating producer config
		logger.info("creating consumer properties");
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // can take following value - earliest, latest or none(gives err)
		
		//creating consumer
		logger.info("creating consumer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		
		//now don't require to subscribe to topic
		
		// assign and seek are mostly used to replay data or fetch a specific message
		
		//assign
		TopicPartition partitionToReadFrom = new TopicPartition(topic, 0); 
		consumer.assign(Arrays.asList(partitionToReadFrom));
		
		//seek
		long offsetToReadFrom = 15L;
		consumer.seek(partitionToReadFrom, offsetToReadFrom);
		
		int noOfMessagesToRead = 5;
		int noOfMessagesReadSoFar = 0;
		
		//polling the data
		logger.info("polling data starts");
		while(noOfMessagesReadSoFar < noOfMessagesToRead) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			
			for(ConsumerRecord<String, String> record : records) {
				logger.info("RECORD: Key: "+record.key() + " Value: "+record.value()+"\n"+
						"Topic: "+record.topic()+" \nPartition: "+record.partition()+
						" Offset: "+record.offset());
				noOfMessagesReadSoFar++;
				if(noOfMessagesReadSoFar >= noOfMessagesToRead)
					break;
			}
		}
		consumer.close();
		logger.info("Exiting...");
	}
	
}
