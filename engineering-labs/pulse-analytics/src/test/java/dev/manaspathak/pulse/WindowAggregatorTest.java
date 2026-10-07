package dev.manaspathak.pulse;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class WindowAggregatorTest {
    @Test void handlesDuplicatesAndLateEvents() {
        WindowAggregator aggregator = new WindowAggregator(Duration.ofMinutes(1), Duration.ofSeconds(10), 100);
        Event impression = new Event("e1", "u1", "c1", Event.Type.IMPRESSION, 120_000, 0);
        assertEquals(WindowAggregator.Outcome.ACCEPTED, aggregator.process(impression));
        assertEquals(WindowAggregator.Outcome.DUPLICATE, aggregator.process(impression));
        aggregator.process(new Event("e2", "u2", "c1", Event.Type.CLICK, 180_000, 0));
        assertEquals(WindowAggregator.Outcome.TOO_LATE,
                aggregator.process(new Event("e3", "u3", "c1", Event.Type.VIEW, 100_000, 0)));
        assertEquals(1, aggregator.snapshot("c1", 10).get(1).impressions());
        assertEquals(1L, aggregator.health().get("duplicates"));
        assertEquals(1L, aggregator.health().get("too_late"));
    }

    @Test void aggregatesConversionsByEventTimeWindow() {
        WindowAggregator aggregator = new WindowAggregator(Duration.ofMinutes(1), Duration.ofMinutes(1), 100);
        aggregator.process(new Event("a", "u", "campaign", Event.Type.CONVERSION, 10_000, 2_000_000));
        aggregator.process(new Event("b", "u", "campaign", Event.Type.CONVERSION, 20_000, 3_000_000));
        var metrics = aggregator.snapshot("campaign", 10).get(0);
        assertEquals(2, metrics.conversions());
        assertEquals(5_000_000, metrics.revenueMicros());
    }
}
