package org.mht.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HighThroughputProducerDemo {

	private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
	private static String topic = "demo_topic";
	private static Logger logger = LoggerFactory.getLogger(SafeProducerDemo.class);
	
	public static void main(String[] args) {
		
		//Create Producer properties
		logger.info("creating kafka properties");
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// create Safe Producer
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		// adding expilitly for confirmation
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
		properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // kafka 2.X >= 1.1 so we can keep this as 5. Otherwise use 1.
		
		// creating high throughput Producer
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "10");
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
		
		
		logger.info("creating kafka consumer");
		//Create Kafka Producer
		final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
		
		for(int i=11; i<22;i++) {
			
			logger.info("creating a producer record");
			//Create Producer Record
			ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "record "+Integer.toString(i));
			
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
			});
		}
		
		//Closing the producer
		producer.close();
	}
	
}
