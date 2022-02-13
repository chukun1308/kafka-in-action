package com.chukun.kafa.action.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class CustomPartitioner implements Partitioner{

	@Override
	public void configure(Map<String, ?> map) {}

	@Override
	public void close() {}

	/**
	 * 控制分区
	 */
	@Override
	public int partition(String str, Object obj1, byte[] b1, Object obj2, byte[] b2, Cluster c) {
		return 0;
	}

}
