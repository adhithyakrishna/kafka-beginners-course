package com.adhithya.kafka.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {
	public static void main(String args[]) {
		// create producer properties
		Properties properties = new Properties();
		String bootstrapServers = "127.0.0.1:9092";
		/*
		 * 
		 * properties.setProperty("bootstrap.servers", bootstrapServers); //kafka
		 * address you have properties.setProperty("key.serializer",
		 * StringSerializer.class.getName()); // what type of data is being sent to
		 * kafka - kafka converts to bytes properties.setProperty("value.serializer",
		 * StringSerializer.class.getName()); // what type of data is being sent to
		 * kafka - kafka converts to bytes
		 */

		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		// create the producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties); // key and value should be string
		
		// send data
		
		//sending needs a producer record to sends
		
		//sending data is asynchronous
		ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello world");		
		producer.send(record);
		//flush data
		producer.flush();
		//flush and close producer
		producer.close();
	}
}
