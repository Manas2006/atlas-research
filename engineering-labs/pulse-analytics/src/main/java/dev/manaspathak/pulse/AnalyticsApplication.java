package dev.manaspathak.pulse;

import java.time.Duration;

public final class AnalyticsApplication {
    public static void main(String[] args) throws Exception {
        String brokers = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        String topic = env("KAFKA_TOPIC", "engagement-events");
        String group = env("KAFKA_GROUP_ID", "pulse-analytics-v1");
        int port = Integer.parseInt(env("HTTP_PORT", "7070"));
        WindowAggregator aggregator = new WindowAggregator(Duration.ofMinutes(1), Duration.ofSeconds(30), 500_000);
        AnalyticsConsumer consumer = new AnalyticsConsumer(brokers, topic, group, aggregator);
        AnalyticsHttpServer http = new AnalyticsHttpServer(port, aggregator);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> { consumer.close(); http.stop(); }));
        http.start();
        consumer.run();
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }
}

