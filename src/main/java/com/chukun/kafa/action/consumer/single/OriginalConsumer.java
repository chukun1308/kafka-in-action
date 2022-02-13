package com.chukun.kafa.action.consumer.single;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * @author chukun
 *  简单的单线程的消费代码示例
 */
public class OriginalConsumer {

    private final Consumer<String, String> kafkaConsumer;

    /**
     * 消息数量
     */
    private final int expectedCount;

    public OriginalConsumer(String brokerId,String topic, String groupId, int expectedCount) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerId);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaConsumer = new KafkaConsumer<>(props);
        this.expectedCount = expectedCount;
    }

    public void run() {
        try{
           int alreadyConsumed = 0;
           while (alreadyConsumed < expectedCount) {
               ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1L));
               alreadyConsumed += records.count();
               records.forEach(this::handleRecord);
           }
        }finally {
            kafkaConsumer.close();
        }
    }

    /**
     * 处理单条消息
     * @param record
     */
    private void handleRecord(ConsumerRecord<String, String> record) {
        try {
            // 模拟每条消息10毫秒处理
            TimeUnit.MILLISECONDS.sleep(ThreadLocalRandom.current().nextInt(10));
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
        System.out.println(Thread.currentThread().getName() + " finished message processed. Record offset = " + record.offset());
    }
}
