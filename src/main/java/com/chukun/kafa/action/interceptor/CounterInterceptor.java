package com.chukun.kafa.action.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * CounterInterceptor 计数
 * @author 初坤
 *
 */
public class CounterInterceptor implements ProducerInterceptor<String, String>{
	
	private int successCount = 0;
	private int errorCount=0;

	@Override
	public void configure(Map<String, ?> map) {}


	@Override
	public void onAcknowledgement(RecordMetadata mataData, Exception exception) {

		if(exception!=null) {
			errorCount++;
		}
		successCount++;
	}

	@Override
	public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
		return record;
	}

	@Override
	public void close() {
		//保存结果
		System.out.println("Successful sent: "+successCount);
		System.out.println("error sent: "+errorCount);
	}

}
