package org.mht.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer {

	public static final String BOOTSTRAP_SERVER = "localhost:9092";
	public static String topic ="demo_topic";
	
	private static Logger logger = LoggerFactory.getLogger(Producer.class);
	
	public static void main(String[] args) {
		
		//Create the Kafka Producer Properties
		logger.info("setting producer configuration properties");
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		logger.info("created producer from the properties");
		//Create the kafka producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		//Send data
		for(int i=0;i<6;i++) {
			String message = "the message is send with id "+Integer.toString(i);
			final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, message);
			logger.info("Sending message: "+message);
			producer.send(record); //asynchronous task
		}
		// close or flush the producer so that the leftover messages are flushed to the broker
		logger.info("Closing the producer");
		producer.flush();
		producer.close();
		
	}
}
