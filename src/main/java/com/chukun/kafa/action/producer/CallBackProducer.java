package com.chukun.kafa.action.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * 创建生产者带回调函数
 * @author 初坤
 *
 */
public class CallBackProducer {
	
	public static void main(String[] args) {
		Properties props = KafkaPropertiesUtils.props();
		
		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
		
		for(int i=0;i<50;i++) {
			ProducerRecord<String, String> record = new ProducerRecord<String, String>("first", "hello"+i);
			kafkaProducer.send(record, new Callback() {
				
				@Override
				public void onCompletion(RecordMetadata metaData, Exception exception) {
					if(metaData!=null) {
						System.out.println(metaData.partition()+"------"+metaData.offset());
					}
				}
			});
		}
		
		kafkaProducer.close();
	}

}
