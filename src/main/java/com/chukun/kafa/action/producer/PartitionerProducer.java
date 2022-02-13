package com.chukun.kafa.action.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * 分区生产者
 * @author 初坤
 *
 */
public class PartitionerProducer {

	public static void main(String[] args) {
		
		Properties props = KafkaPropertiesUtils.props();
		
		props.put("partitioner.class", "com.chukun.kafka.CustomPartitioner");
		
		Producer<String, String> producer = new KafkaProducer<>(props);
		
		producer.send(new ProducerRecord<String, String>("first","1", "chukun"));
		
		producer.close();
	}
}
