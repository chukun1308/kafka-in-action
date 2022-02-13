package com.chukun.kafa.action.consumer.multi;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * @author chukun
 * 监听ConsumerRebalance
 */
public class MultiThreadRebalancedListener implements ConsumerRebalanceListener {

    // Consumer实例
    private final Consumer<String, String> kafkaConsumer;
    // 要停掉的Worker线程实例
    private final Map<TopicPartition, ConsumerWorker<String, String>> outstandingWorkers;
    // 要提交的位移数据
    private final Map<TopicPartition, OffsetAndMetadata> offsets;

    public MultiThreadRebalancedListener(Consumer<String, String> consumer,
                                          Map<TopicPartition, ConsumerWorker<String, String>> outstandingWorkers,
                                          Map<TopicPartition, OffsetAndMetadata> offsets) {
        this.kafkaConsumer = consumer;
        this.outstandingWorkers = outstandingWorkers;
        this.offsets = offsets;
    }


    /**
     * 发生rebalance之前调用
     * @param partitions
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
       Map<TopicPartition, ConsumerWorker<String,String>> stoppedWorkers = new HashMap<>();
       // 获取未提交的位移，并且关闭worker
        for (TopicPartition tp : partitions) {
            ConsumerWorker<String, String> worker = outstandingWorkers.remove(tp);
            if (!Objects.isNull(worker)) {
                worker.close();
                stoppedWorkers.put(tp, worker);
            }
        }
        // 停止的worker，获取这些worker的offset
        stoppedWorkers.forEach((tp,worker) -> {
            long offset  = worker.waitForCompletion(1, TimeUnit.SECONDS);
            if (offset > 0) {
                offsets.put(tp, new OffsetAndMetadata(offset));
            }
        });
        Map<TopicPartition, OffsetAndMetadata> revokedOffsets = new HashMap<>();
        // 把这些停止的worker的offset放入revokedOffsets里面
        partitions.forEach(tp -> {
            OffsetAndMetadata offsetAndMetadata = offsets.remove(tp);
            if (!Objects.isNull(offsetAndMetadata)) {
                revokedOffsets.put(tp, offsetAndMetadata);
            }
        });

        try{
            // 提交这些位移
            kafkaConsumer.commitSync(revokedOffsets);
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * rebalance完成之后被调用
     * @param partitions
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        kafkaConsumer.resume(partitions);
    }
}
