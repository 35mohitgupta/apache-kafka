package org.mht.kafka.producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerWithKeyDemo {

	private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
	private static String topic = "demo_topic";
	private static Logger logger = LoggerFactory.getLogger(ProducerCallbackDemo.class);
	
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		
		//Create Producer properties
		logger.info("creating kafka properties");
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		logger.info("creating kafka consumer");
		//Create Kafka Producer
		final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		for(int i=11; i<22;i++) {
			
			String value="record "+Integer.toString(i);
			String key="id_"+Integer.toString(i);
			
			logger.info("creating a producer record");
			//Create Producer Record
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
			
			logger.info("KEY: "+key);
			
			//Send data - asynchronous
			producer.send(record, new Callback() {
				// This executes every time the record is sent or there is an exception
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if(exception == null) {
						logger.info("Received new METADATA: \n"
								+ "Topic: "+metadata.topic()+"\n"
								+"Partition: "+metadata.partition()+"\n"
								+"Offset: "+metadata.offset()+"\n"
								+"Timestamp: "+metadata.timestamp());
					}else {
						logger.error("Exception occured while sending record : "+exception);
					}
				}
			}).get(); // .get blobks the .send() to make it synchronous - never do this in production
		}
		
		//Closing the producer
		producer.close();
	}
	
}
