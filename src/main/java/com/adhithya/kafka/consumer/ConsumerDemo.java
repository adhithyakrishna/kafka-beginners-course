package com.adhithya.kafka.consumer;

import java.time.Duration;
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

	public static void main(String[] args) {
		Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
		
		Properties properties = new Properties();
		
		String bootstrapServers = "127.0.0.1:9092";
		String groupId = "my-fourth-application";
		String topic = "first_topic";
				
		
		
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId); 
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		//earliest - from the beginning
		//latest - only the new messages
		//none = will throw an error when no offset is being saved
		
		//create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
		
		// subscribe consumer to our topic
		consumer.subscribe(Collections.singleton(topic)); // only subscribe to one topic
		//Arrays.asList("first_topic", "second_topic") //subscribe to multiple topics
		
		
		//poll for the new data
		while(true) //
		{
			
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100)); //complete in 100 ms
			
			for(ConsumerRecord<String, String> record : records)
			{
				logger.info("Key : " + record.key() + ", Value: " + record.value());
				logger.info("Partition: " + record.partition() + ", offset" + record.offset());
			}
		}

	}

}
