package com.adhithya.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallback {
	public static void main(String args[]) {
		
		final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
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
		producer.send(record, new Callback() {
			//executes everytime a record is being sent successfully or an exception is thrown
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				if(exception==null)
				{
					logger.info("Recieved new metadat : \n" + "Topic :" + metadata.topic() +", ProducerDemo.javaPartition :" + metadata.partition() + ", Offset : " + metadata.offset() + ", Time stamp : " +metadata.timestamp());
					// record sent successfully
				}
				else {
					//error has to be dealt
					logger.error("Error while producing", exception);
				}
				
			}
		});
		//flush data
		producer.flush();
		//flush and close producer
		producer.close();
	}
}
