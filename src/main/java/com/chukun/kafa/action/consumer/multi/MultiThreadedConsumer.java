package com.chukun.kafa.action.consumer.multi;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/**
 * Consumer的控制主类
 */
public class MultiThreadedConsumer {

    private final Map<TopicPartition,ConsumerWorker<String,String>> outstandingWorkers = new HashMap<>();
    private final Map<TopicPartition, OffsetAndMetadata> offsets2Commit = new HashMap<>();
    private long lastCommitTime = System.currentTimeMillis();
    private final Consumer<String,String> kafkaConsumer;
    private final int DEFAULT_COMMIT_INTERVAL = 3000;
    private final Map<TopicPartition, Long> currentConsumedOffsets = new HashMap<>();
    private final long expectedCount;

    /**
     * 创建了一个包含10倍CPU核数的线程数。
     * 具体线程数根据业务需求而定。
     * 如果处理逻辑是I/O密集型操作（比如写入外部系统），那么设置一个大一点的线程数通常都是有意义的
     */
    private final static Executor EXECUTOR = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors() * 10, r -> {
                Thread t = new Thread();
                t.setDaemon(true);
                return t;
            });

    public MultiThreadedConsumer(String brokerId,String topic,String groupID, long expectedCount) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerId);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 将自动提交位移的参数设置为false。多线程Consumer的一个关键设计就是要手动提交位移
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        kafkaConsumer = new KafkaConsumer<>(props);
        // Rebalance监听器设置为MultiThreadRebalancedListener
        kafkaConsumer.subscribe(Collections.singleton(topic), new MultiThreadRebalancedListener(kafkaConsumer,outstandingWorkers,offsets2Commit));
        this.expectedCount =expectedCount;
    }

    /**
     * run方法的逻辑基本上遵循了上面提到的流程：消息获取 -> 分发 -> 检查消费进度 -> 提交位移
     */
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
                distributeRecords(records);
                checkOutstandingWorkers();
                commitOffsets();
                if (currentConsumedOffsets.values().stream().mapToLong(Long::longValue).sum() >= expectedCount) {
                    break;
                }
            }
        } finally {
            kafkaConsumer.close();
        }
    }

    /**
     * 对已完成消息处理并提交位移的分区执行resume操作
     */
    private void checkOutstandingWorkers() {
        Set<TopicPartition> completedPartitions = new HashSet<>();
        outstandingWorkers.forEach((tp, worker) -> {
            if (worker.isFinished()) {
                completedPartitions.add(tp);
            }
            long offset = worker.getLatestProcessedOffset();
            currentConsumedOffsets.put(tp, offset);
            if (offset > 0L) {
                offsets2Commit.put(tp, new OffsetAndMetadata(offset));
            }
        });
        completedPartitions.forEach(outstandingWorkers::remove);
        // 恢复暂停的分区
        kafkaConsumer.resume(completedPartitions);
    }

    /**
     * 提交位移
     */
    private void commitOffsets() {
        try {
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastCommitTime > DEFAULT_COMMIT_INTERVAL && !offsets2Commit.isEmpty()) {
                kafkaConsumer.commitSync(offsets2Commit);
                offsets2Commit.clear();
            }
            lastCommitTime = currentTime;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 将不同分区的消息交由不同的线程，同时暂停该分区消息消费
     * @param records
     */
    private void distributeRecords(ConsumerRecords<String, String> records) {
        if (records.isEmpty()) {
            return;
        }
        Set<TopicPartition> pausedPartitions = new HashSet<>();
        records.partitions().forEach(tp -> {
            List<ConsumerRecord<String, String>> partitionedRecords = records.records(tp);
            pausedPartitions.add(tp);
            final ConsumerWorker<String, String> worker = new ConsumerWorker<>(partitionedRecords);
            CompletableFuture.supplyAsync(worker::run, EXECUTOR);
            outstandingWorkers.put(tp, worker);
        });
        // 暂停这些分区
        kafkaConsumer.pause(pausedPartitions);
    }
}
