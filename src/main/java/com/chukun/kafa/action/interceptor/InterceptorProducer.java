package com.chukun.kafa.action.interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class InterceptorProducer {
	
	public static void main(String[] args) {
		Properties props = KafkaPropertiesUtils.props();
		//构建拦截器链
		List<String> interceptorList = new ArrayList<>();
		interceptorList.add("com.chukun.kafka.TimeInterceptor");
		interceptorList.add("com.chukun.kafka.CounterInterceptor");
		props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptorList);
		
		String topic="first";
		Producer<String, String> producer = new KafkaProducer<>(props);
		
		//发送消息
		for(int i=0;i<10;i++) {
			ProducerRecord<String, String>  record = new ProducerRecord<String, String>(topic, "message"+i);
			producer.send(record);
		}
		// 4 一定要关闭producer，这样才会调用interceptor的close方法
		producer.close();
	}

}
