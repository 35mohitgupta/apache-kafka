package org.mht.kafka.consumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {

	private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
	private static final String topic = "demo_topic";
	private static final String groupName = "my-first-java-app";
	private static Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
	
	public static void main(String[] args) {
		
		//creating producer config
		logger.info("creating consumer properties");
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupName);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // can take following value - earliest, latest or none(gives err)
		
		//creating consumer
		logger.info("creating consumer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		
		//subscribing topic
		logger.info("subscribing to topic");
		consumer.subscribe(Collections.singleton(topic));
		// or
//		consumer.subscribe(Arrays.asList(topic));
		
		//polling the data
		logger.info("polling data starts");
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			
			for(ConsumerRecord<String, String> record : records) {
				logger.info("RECORD: Key: "+record.key() + " Value: "+record.value()+"\n"+
						"Topic: "+record.topic()+" \nPartition: "+record.partition()+
						" Offset: "+record.offset());
			}
		}
		
//		consumer.close();
	}
	
}
