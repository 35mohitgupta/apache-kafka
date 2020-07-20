package org.mht.kafka.consumer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ManualOffsetCommitConsumerDemo {
	private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
	private static final String topic = "demo_topic";
	private static final String groupName = "idempotent-java-app";
	private static Logger logger = LoggerFactory.getLogger(IdempotentConsumerDemo.class);
	private static Map<String, String> recordMap;
	
	private static final String fileName = "kafka_records"; 
	
	public static void main(String[] args) throws IOException {
		
		//creating producer config
		logger.info("creating consumer properties");
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupName);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // can take following value - earliest, latest or none(gives err)
		//limiting the no of records fetched after polling request
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "25");
		// disabling auto commit
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		
		
		//creating consumer
		logger.info("creating consumer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		
		File file = getFile();
		
		//initialize map
		initializeMap(file);
		
		
		//subscribing topic
		logger.info("subscribing to topic");
		consumer.subscribe(Collections.singleton(topic));
		// or
//		consumer.subscribe(Arrays.asList(topic));
		
		//polling the data
		logger.info("polling data starts");
		while(true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			logger.info("No of records fetched during poll "+records.count());
			for(ConsumerRecord<String, String> record : records) {
				
				String mapKey = record.topic()+"_"+ record.partition()+"_"+ record.offset();
				
				logger.info("RECORD: Key: "+record.key() + " Value: "+record.value()+"\n"+
						"Topic: "+record.topic()+" \nPartition: "+record.partition()+
						" Offset: "+record.offset());
				if(recordMap.containsKey(mapKey)) {
					logger.info("Message is already processed "+record.value());
				}else {
					recordMap.put(mapKey, record.value());
					saveRecord(mapKey, record.value(), file);
				}
			}
			logger.info("Commiting offsets");
			consumer.commitSync();
			logger.info("Offset Commited");
		}
		
//		consumer.close();
	}


	private static File getFile() throws IOException {
		File file =  new File(fileName);
		if(!file.exists()) {
			file.createNewFile();
		}
		return file;
	}


	private static void initializeMap(File file) throws FileNotFoundException {
		recordMap = new HashMap<String, String>();
		if(!file.exists())
			return;
		Scanner reader = new Scanner(file);
		while(reader.hasNext()) {
			String nextline = reader.nextLine();
			logger.info("nextLine: ",nextline);
			String[] keyVal = nextline.split(",");
			recordMap.put(keyVal[0], keyVal[1]);
		}	
		reader.close();
	}
	
	
	private static void saveRecord(String key, String message, File file) throws IOException {
		FileWriter fileWriter = new FileWriter(file,true);
		String data = key+","+message+"\n";
		fileWriter.write(data);
		fileWriter.close();
	}
	
	
}
