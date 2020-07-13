package org.mht.kafka.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerWithFramework {


	private Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
	
	public static void main(String[] args) {
		
		new ConsumerWithFramework().run();
	}
	
	public void run() {
		final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
		final String topic = "demo_topic";
		final String groupName = "my-first-java-app";
		
		// Latch for dealing multiple threads
		CountDownLatch countDownLatch = new CountDownLatch(1);
		
		//creating consumer runnable
		logger.info("Creating Consumer Runnable");
		final Runnable consumerRunnable = new ConsumerRunnable(countDownLatch, BOOTSTRAP_SERVER, groupName, topic);
		Thread  consumerThread = new Thread(consumerRunnable);
		
		//starting the consumer thread
		logger.info("Starting the consumer thread");
		consumerThread.start();
		
		//adding shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(()->  {
			logger.info("Cought shutdown hook");
			((ConsumerRunnable) consumerRunnable).shutdown();
			try {
				countDownLatch.await();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			logger.info("Application has exited");
		}));
		
		
		try {
			// await() waits for other thread to stop or call countDown()
			countDownLatch.await();
		} catch (InterruptedException e) {
			logger.info("Application got interrupted "+e);
		}finally {
			logger.info("Aplication is closing");
		}
	}
	
	public class ConsumerRunnable implements Runnable{

		private CountDownLatch countDownLatch;
		private KafkaConsumer<String, String> consumer;
		private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);
		
		public ConsumerRunnable(CountDownLatch countDownLatch,
							 String bootstrapServer,
							 String groupName,
							 String topic) {
			
			Properties properties = new Properties();
			properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
			properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
			properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupName);
			properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // can take following value - earliest, latest or none(gives err)
			
			//creating consumer
			consumer = new KafkaConsumer<String, String>(properties);
			
			//subscribing topic
			consumer.subscribe(Collections.singleton(topic));
			
			this.countDownLatch = countDownLatch;
		}
		
		public void run() {
			//polling the data
			logger.info("polling data starts");
			try {
				while(true) {
					ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
					
					for(ConsumerRecord<String, String> record : records) {
						logger.info("RECORD: Key: "+record.key() + " Value: "+record.value()+"\n"+
								"Topic: "+record.topic()+" \nPartition: "+record.partition()+
								" Offset: "+record.offset());
					}
				}
			}catch (WakeupException e) {
				logger.info("Received shutdown signal");
			}finally {
				// tell our main code we 're done with with consumer
				consumer.close();
				countDownLatch.countDown();
			}
		}
		
		public void shutdown() {
			// the wakeup() method is a special method top interrupt consumer.poll()
			// it will throw the WakeUpException
			consumer.wakeup();
		}
		
	}
	
}
