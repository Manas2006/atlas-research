package dev.manaspathak.pulse;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class WindowAggregator {
    public enum Outcome { ACCEPTED, DUPLICATE, TOO_LATE }

    public record WindowKey(String campaignId, long startMillis) {}

    public record Metrics(
            String campaignId,
            long windowStartMillis,
            long windowEndMillis,
            long impressions,
            long clicks,
            long views,
            long conversions,
            long revenueMicros) {

        Metrics add(Event event) {
            return new Metrics(campaignId, windowStartMillis, windowEndMillis,
                    impressions + (event.type() == Event.Type.IMPRESSION ? 1 : 0),
                    clicks + (event.type() == Event.Type.CLICK ? 1 : 0),
                    views + (event.type() == Event.Type.VIEW ? 1 : 0),
                    conversions + (event.type() == Event.Type.CONVERSION ? 1 : 0),
                    revenueMicros + (event.type() == Event.Type.CONVERSION ? event.valueMicros() : 0));
        }
    }

    private final long windowMillis;
    private final long allowedLatenessMillis;
    private final int dedupCapacity;
    private final Map<WindowKey, Metrics> windows = new LinkedHashMap<>();
    private final LinkedHashMap<String, Boolean> seenEventIds;
    private long maxEventTime = Long.MIN_VALUE;
    private long duplicates;
    private long tooLate;

    public WindowAggregator(Duration windowSize, Duration allowedLateness, int dedupCapacity) {
        if (windowSize.isZero() || windowSize.isNegative()) throw new IllegalArgumentException("window must be positive");
        if (dedupCapacity < 1) throw new IllegalArgumentException("dedup capacity must be positive");
        this.windowMillis = windowSize.toMillis();
        this.allowedLatenessMillis = allowedLateness.toMillis();
        this.dedupCapacity = dedupCapacity;
        this.seenEventIds = new LinkedHashMap<>(dedupCapacity + 1, 0.75f, true);
    }

    public synchronized Outcome process(Event event) {
        if (seenEventIds.containsKey(event.eventId())) {
            duplicates++;
            return Outcome.DUPLICATE;
        }
        seenEventIds.put(event.eventId(), Boolean.TRUE);
        if (seenEventIds.size() > dedupCapacity) {
            var iterator = seenEventIds.entrySet().iterator();
            iterator.next();
            iterator.remove();
        }

        long watermark = maxEventTime == Long.MIN_VALUE ? Long.MIN_VALUE : maxEventTime - allowedLatenessMillis;
        if (event.eventTimeMillis() < watermark) {
            tooLate++;
            return Outcome.TOO_LATE;
        }
        maxEventTime = Math.max(maxEventTime, event.eventTimeMillis());
        long start = Math.floorDiv(event.eventTimeMillis(), windowMillis) * windowMillis;
        WindowKey key = new WindowKey(event.campaignId(), start);
        Metrics empty = new Metrics(event.campaignId(), start, start + windowMillis, 0, 0, 0, 0, 0);
        windows.put(key, windows.getOrDefault(key, empty).add(event));
        return Outcome.ACCEPTED;
    }

    public synchronized List<Metrics> snapshot(String campaignId, int limit) {
        List<Metrics> result = new ArrayList<>();
        for (Metrics metrics : windows.values()) {
            if (campaignId == null || campaignId.isBlank() || campaignId.equals(metrics.campaignId())) {
                result.add(metrics);
            }
        }
        result.sort(Comparator.comparingLong(Metrics::windowStartMillis).reversed());
        return result.size() <= limit ? result : new ArrayList<>(result.subList(0, limit));
    }

    public synchronized Map<String, Long> health() {
        return Map.of(
                "active_windows", (long) windows.size(),
                "dedup_entries", (long) seenEventIds.size(),
                "duplicates", duplicates,
                "too_late", tooLate,
                "watermark", maxEventTime == Long.MIN_VALUE ? 0 : maxEventTime - allowedLatenessMillis);
    }
}

