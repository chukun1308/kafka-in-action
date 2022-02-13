package com.chukun.kafa.action.streams;

import java.util.Properties;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

/**
 * 主程序
 * @author 初坤
 *
 */
public class Application {
	
	public static void main(String[] args) {
		// 定义输入的topic
		String fromTopic = "first";
		 // 定义输出的topic
        String toTopic = "second";
        
        // 设置参数
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop01:9092");
        
        StreamsConfig config = new StreamsConfig(settings);
        
        // 构建拓扑
        TopologyBuilder  topologyBuilder = new TopologyBuilder();
        
        topologyBuilder
                .addSource("source", fromTopic)
                .addProcessor("processor", new ProcessorSupplier<byte[], byte[]>() {

					@Override
					public Processor<byte[], byte[]> get() {
						return new LogProcessor();
					}
				},"source")
                .addSink("sink", toTopic, "processor");
        
        // 创建kafka stream
        KafkaStreams streams = new KafkaStreams(topologyBuilder, config);
        streams.start();
                   
	}

}
