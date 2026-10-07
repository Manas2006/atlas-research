package dev.manaspathak.pulse;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public final class AnalyticsConsumer implements AutoCloseable {
    private final KafkaConsumer<String, String> consumer;
    private final WindowAggregator aggregator;
    private final String topic;
    private final AtomicBoolean running = new AtomicBoolean(true);

    public AnalyticsConsumer(String bootstrapServers, String topic, String groupId, WindowAggregator aggregator) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "300000");
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        this.consumer = new KafkaConsumer<>(properties);
        this.aggregator = aggregator;
        this.topic = topic;
    }

    public void run() {
        consumer.subscribe(java.util.List.of(topic), new ConsumerRebalanceListener() {
            @Override public void onPartitionsRevoked(Collection<TopicPartition> partitions) { consumer.commitSync(); }
            @Override public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}
        });
        try {
            while (running.get()) {
                var records = consumer.poll(Duration.ofMillis(250));
                for (var record : records) {
                    aggregator.process(Event.decode(record.value()));
                }
                if (!records.isEmpty()) {
                    // Offset commits occur only after every record in the batch is applied.
                    // A crash before this point replays records, which the dedup store absorbs.
                    consumer.commitSync(Duration.ofSeconds(2));
                }
            }
        } finally {
            consumer.close(Duration.ofSeconds(5));
        }
    }

    @Override public void close() {
        running.set(false);
        consumer.wakeup();
    }
}
