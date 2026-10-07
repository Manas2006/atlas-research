package dev.manaspathak.pulse;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public final class AnalyticsHttpServer {
    private final HttpServer server;
    private final WindowAggregator aggregator;

    public AnalyticsHttpServer(int port, WindowAggregator aggregator) throws IOException {
        this.aggregator = aggregator;
        server = HttpServer.create(new InetSocketAddress(port), 128);
        server.createContext("/healthz", this::health);
        server.createContext("/v1/metrics", this::metrics);
        server.setExecutor(java.util.concurrent.Executors.newFixedThreadPool(
                Math.max(2, Runtime.getRuntime().availableProcessors())));
    }

    public void start() { server.start(); }
    public void stop() { server.stop(2); }

    private void health(HttpExchange exchange) throws IOException {
        write(exchange, 200, mapJson(aggregator.health()));
    }

    private void metrics(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) { write(exchange, 405, "{\"error\":\"method not allowed\"}"); return; }
        Map<String, String> query = parseQuery(exchange.getRequestURI().getRawQuery());
        int limit;
        try { limit = Math.min(1000, Math.max(1, Integer.parseInt(query.getOrDefault("limit", "100")))); }
        catch (NumberFormatException ignored) { limit = 100; }
        StringBuilder json = new StringBuilder("{\"windows\":[");
        boolean first = true;
        for (var metric : aggregator.snapshot(query.get("campaign"), limit)) {
            if (!first) json.append(',');
            first = false;
            json.append(String.format(java.util.Locale.ROOT,
                    "{\"campaign_id\":\"%s\",\"window_start_ms\":%d,\"window_end_ms\":%d,\"impressions\":%d,\"clicks\":%d,\"views\":%d,\"conversions\":%d,\"revenue_micros\":%d}",
                    escape(metric.campaignId()), metric.windowStartMillis(), metric.windowEndMillis(), metric.impressions(), metric.clicks(), metric.views(), metric.conversions(), metric.revenueMicros()));
        }
        json.append("]}");
        write(exchange, 200, json.toString());
    }

    private static Map<String, String> parseQuery(String raw) {
        Map<String, String> result = new HashMap<>();
        if (raw == null || raw.isBlank()) return result;
        for (String pair : raw.split("&")) {
            String[] parts = pair.split("=", 2);
            result.put(URLDecoder.decode(parts[0], StandardCharsets.UTF_8),
                    parts.length == 2 ? URLDecoder.decode(parts[1], StandardCharsets.UTF_8) : "");
        }
        return result;
    }

    private static String mapJson(Map<String, Long> values) {
        StringBuilder result = new StringBuilder("{");
        boolean first = true;
        for (var entry : values.entrySet()) {
            if (!first) result.append(',');
            first = false;
            result.append('"').append(escape(entry.getKey())).append("\":").append(entry.getValue());
        }
        return result.append('}').toString();
    }

    private static String escape(String value) { return value.replace("\\", "\\\\").replace("\"", "\\\""); }

    private static void write(HttpExchange exchange, int status, String body) throws IOException {
        byte[] payload = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, payload.length);
        exchange.getResponseBody().write(payload);
        exchange.close();
    }
}
