package com.chukun.kafa.action.consumer.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class KafkaNewConsumer {
	
	public static void main(String[] args) {
		
		Properties props = KafkaPropertiesUtils.props();
		// 定义consumer 
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		
		//消费者订阅的topic, 可同时订阅多个
		consumer.subscribe(Arrays.asList("first", "second","third"));
		
		while(true) {
			//读取数据，读取超时时间为100ms
			ConsumerRecords<String,String> records = consumer.poll(100);
			for(ConsumerRecord<String, String> record:records) {
			System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
			}
		}
	}

}
