package com.chukun.kafa.action.quickstart;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * @author chukun
 * kafka stream简单使用
 */
public class KafkaStreamDemo {

    private static final String BROKER_LIST = "linux01:9092";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"stream-word-count001");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<Object, Object> source = builder.stream("streams-plaintext-input");
        KTable<String, Long> counts = source.flatMapValues(value -> Arrays.asList(value.toString().toLowerCase(Locale.getDefault()).split("\\s+")))
                .groupBy((key, value) -> value)
                .count();

        counts.toStream().to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));

        final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties);

        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            kafkaStreams.close();
            latch.countDown();
        }, "streams-wordcount-output"));

        try {
            kafkaStreams.start();
            latch.await();
        }catch (Exception e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
