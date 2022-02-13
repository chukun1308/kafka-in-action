package com.chukun.kafa.action.consumer.example;

import java.util.Properties;

public class KafkaPropertiesUtils {
	
	public static Properties props() {
		Properties props = new Properties();
		// 定义kakfa 服务的地址，不需要将所有broker指定上 
		props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
		// 制定consumer group 
		props.put("group.id", "test");
		// 是否自动确认offset 
		props.put("enable.auto.commit", "true");
		// 自动确认offset的时间间隔 
		props.put("auto.commit.interval.ms", "1000");
		// key的序列化类
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		// value的序列化类 
		props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
		return props;
	}

}
