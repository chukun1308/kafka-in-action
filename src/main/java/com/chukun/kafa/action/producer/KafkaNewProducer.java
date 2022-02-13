package com.chukun.kafa.action.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * kafka生产者
 * @author 初坤
 *
 */
public class KafkaNewProducer {

	
	public static void main(String[] args) {
		
		Properties props = KafkaPropertiesUtils.props();
		KafkaProducer<String, String> producer = new KafkaProducer<>(props);
		
		for(int i=0; i<50;i++) {
			ProducerRecord<String, String> record = new ProducerRecord<String, String>("first", Integer.toString(i), "hello"+i);
			producer.send(record);
		}
		
		producer.close();
	}
	
}
