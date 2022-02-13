package com.chukun.kafa.action.consumer.multi;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 每个Worker只处理相同分区下的消息
 * @param <K>
 * @param <V>
 */
public class ConsumerWorker<K, V> {

    private final List<ConsumerRecord<K,V>> recordsOfSamePartition;

    private volatile boolean started = false;
    private volatile boolean stopped = false;

    private ReentrantLock lock = new ReentrantLock();

    private final long INVALID_COMMITTED_OFFSET = -1;
    /**
     * 使用这个变量保存该Worker当前已消费的最新位移
     */
    private final AtomicLong latestProcessedOffset = new AtomicLong(INVALID_COMMITTED_OFFSET);
    /**
     * 使用CompletableFuture来保存Worker要提交的位移
     */
    private final CompletableFuture<Long> future = new CompletableFuture<>();

    public ConsumerWorker(List<ConsumerRecord<K,V>> recordsOfSamePartition) {
        this.recordsOfSamePartition = recordsOfSamePartition;
    }

    public boolean run() {
        lock.lock();
        try{
            if (stopped) {
                return false;
            }
            this.started = true;
        }finally {
           lock.unlock();
        }
        for (ConsumerRecord<K, V> record : recordsOfSamePartition) {
            if (stopped) {
                break;
            }
            handleRecord(record);
            if (latestProcessedOffset.get() < record.offset() + 1) {
                latestProcessedOffset.set(record.offset() + 1);
            }
        }
        // Worker成功操作与否的标志就是看这个future是否将latestProcessedOffset值封装到结果中
        return future.complete(latestProcessedOffset.get());
    }

    public long getLatestProcessedOffset() {
        return latestProcessedOffset.get();
    }

    private void handleRecord(ConsumerRecord<K,V> record) {
        try {
            // 模拟每条消息10毫秒处理
            TimeUnit.MILLISECONDS.sleep(ThreadLocalRandom.current().nextInt(10));
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
        System.out.println(Thread.currentThread().getName() + " finished message processed. Record offset = " + record.offset());
    }

    /**
     * 关闭worker
     */
    public void close() {
        lock.lock();
        this.stopped = true;
        if (!started) {
            future.complete(latestProcessedOffset.get());
        }
        lock.unlock();
    }

    /**
     * 判断future是否完成
     * @return
     */
    public boolean isFinished() {
        return future.isDone();
    }

    /**
     * 等待一定的时长，future完成
     * @param time
     * @param timeUnit
     * @return
     */
    public long waitForCompletion(long time, TimeUnit timeUnit) {
        try{
           return future.get(time, timeUnit);
        }catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return INVALID_COMMITTED_OFFSET;
        }
    }
}
