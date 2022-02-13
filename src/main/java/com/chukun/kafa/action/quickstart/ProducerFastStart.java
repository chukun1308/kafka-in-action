package com.chukun.kafa.action.quickstart;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * kafka 发送消息快速入门
 *
 * @author chukun
 */
public class ProducerFastStart {

    private static final String BROKER_LIST = "linux01:9092";
    private static final String TOPIC = "topic-demo";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        // 生成客户端的kafkaProducer实例
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        //构建需要发送的消息
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC, "hello,first kafka");

        try {
            // 发送消息
            kafkaProducer.send(producerRecord);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //关闭kafka客户端
            kafkaProducer.close();
        }

        // 使用try-resource语法糖
//        try(KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(properties)) {
//            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC, "hello,first kafka");
//            kafkaProducer.send(producerRecord);
//        }catch (Exception e) {
//            e.printStackTrace();
//        }

    }
}
