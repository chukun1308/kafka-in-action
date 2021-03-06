package com.chukun.kafa.action.producer;

import java.util.Properties;

/**
 * 公共配置
 * @author 初坤
 *
 */
public class KafkaPropertiesUtils {
	
	/**
	 * 获取配置信息
	 * @return
	 */
	public static Properties props() {
		Properties props = new Properties();
		// Kafka服务端的主机名和端口号
		props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
		// 等待所有副本节点的应答
		props.put("acks", "all");
		// 消息发送最大尝试次数
		props.put("retries", 0);
		// 一批消息处理大小
		props.put("batch.size", 16384);
		// 请求延时
		props.put("linger.ms", 1);
		// 发送缓存区内存大小
		props.put("buffer.memory", 33554432);
		// key序列化
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		// value序列化
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		return props;
	}

}
