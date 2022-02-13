package com.chukun.kafa.action.consumer.example;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class ConsumerControlMessageLag {

    public static void main(String[] args) {

    }

    /**
     * 社区提供的 Java Consumer API 分别提供了查询当前分区最新消息位移和消费者组最新消费消息位移两组方法，使用它们就能计算出对应的 Lag
     * @param groupID
     * @param bootstrapServers
     * @return
     * @throws TimeoutException
     */
    public static Map<TopicPartition,Long>  lagOf(String groupID,String bootstrapServers) throws TimeoutException {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        try(AdminClient adminClient = AdminClient.create(props)){
            ListConsumerGroupOffsetsResult result = adminClient.listConsumerGroupOffsets(groupID);
            KafkaFuture<Map<TopicPartition, OffsetAndMetadata>> mapKafkaFuture = result.partitionsToOffsetAndMetadata();
            try {
                Map<TopicPartition, OffsetAndMetadata> consumedOffsets = mapKafkaFuture.get(10, TimeUnit.SECONDS);
                //禁止自动提交位移
                props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,false);
                //配置groupID
                props.put(ConsumerConfig.GROUP_ID_CONFIG,groupID);
                //配置key value的序列化方式
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
                try(KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<>(props)){
                    Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(consumedOffsets.keySet());
                    return endOffsets.entrySet().stream().collect(Collectors.toMap(entry->entry.getKey(),
                             entry->entry.getValue()-consumedOffsets.get(entry.getKey()).offset()));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                // 处理中断异常
                // ...
                return Collections.emptyMap();
            } catch (ExecutionException e) {
                // 处理ExecutionException
                // ...
                return Collections.emptyMap();
            } catch (TimeoutException e) {
                throw new TimeoutException("Timed out when getting lag for consumer group " + groupID);
            }
        }
    }
}
