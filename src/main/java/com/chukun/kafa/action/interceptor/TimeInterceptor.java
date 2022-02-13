package com.chukun.kafa.action.interceptor;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * 自定义拦截器
 * @author 初坤
 *
 */

public class TimeInterceptor implements ProducerInterceptor<String, String>{
	@Override
	public void configure(Map<String, ?> map) {}

	@Override
	public void close() {}

	@Override
	public void onAcknowledgement(RecordMetadata metaData, Exception e) {}

	@Override
	public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
		
		ProducerRecord<String, String> producerRecord = 
				new ProducerRecord<String, String>(
						record.topic(), 
						record.partition(), 
						record.timestamp(),
						record.key(),
						System.currentTimeMillis() + "," + record.value().toString());
		
		return producerRecord;
	}

}
