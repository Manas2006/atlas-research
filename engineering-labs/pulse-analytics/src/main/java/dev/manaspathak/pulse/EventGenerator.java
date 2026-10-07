package dev.manaspathak.pulse;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

public final class EventGenerator {
    public static void main(String[] args) throws Exception {
        String brokers = args.length > 0 ? args[0] : "localhost:9092";
        String topic = args.length > 1 ? args[1] : "engagement-events";
        int eventsPerSecond = args.length > 2 ? Integer.parseInt(args[2]) : 1_000;
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
        try (var producer = new KafkaProducer<String, String>(properties)) {
            long intervalNanos = 1_000_000_000L / eventsPerSecond;
            long next = System.nanoTime();
            while (true) {
                Event event = randomEvent();
                producer.send(new ProducerRecord<>(topic, event.campaignId(), event.encode()));
                next += intervalNanos;
                long wait = next - System.nanoTime();
                if (wait > 0) java.util.concurrent.locks.LockSupport.parkNanos(wait);
            }
        }
    }

    private static Event randomEvent() {
        var random = ThreadLocalRandom.current();
        Event.Type type = Event.Type.values()[random.nextInt(Event.Type.values().length)];
        return new Event(UUID.randomUUID().toString(), "user-" + random.nextInt(100_000),
                "campaign-" + random.nextInt(100), type, System.currentTimeMillis(),
                type == Event.Type.CONVERSION ? random.nextLong(1_000_000, 100_000_000) : 0);
    }
}

