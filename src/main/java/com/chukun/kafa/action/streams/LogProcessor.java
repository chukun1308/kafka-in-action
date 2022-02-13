package com.chukun.kafa.action.streams;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 * log 预处理
 * @author 初坤
 *
 */
public class LogProcessor implements Processor<byte[], byte[]>{

	private ProcessorContext context;
	@Override
	public void close() {}

	@Override
	public void init(ProcessorContext context) {
		this.context = context;
	}

	@Override
	public void process(byte[] key, byte[] value) {
		String input = new String(value);
		// 如果包含“>>>”则只保留该标记后面的内容
		if(input.contains(">>>")) {
			input = input.replace(">>>", "-->");
			// 输出到下一个topic
			context.forward("logProcessor".getBytes(), input.getBytes());
		}else {
			context.forward("logProcessor".getBytes(), input.getBytes());
		}
	}

}
