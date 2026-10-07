package dev.manaspathak.pulse;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

public record Event(
        String eventId,
        String userId,
        String campaignId,
        Type type,
        long eventTimeMillis,
        long valueMicros) {

    public enum Type { IMPRESSION, CLICK, VIEW, CONVERSION }

    public Event {
        Objects.requireNonNull(eventId);
        Objects.requireNonNull(userId);
        Objects.requireNonNull(campaignId);
        Objects.requireNonNull(type);
    }

    public String encode() {
        return String.join("\t",
                b64(eventId), b64(userId), b64(campaignId), type.name(),
                Long.toString(eventTimeMillis), Long.toString(valueMicros));
    }

    public static Event decode(String value) {
        String[] fields = value.split("\t", -1);
        if (fields.length != 6) throw new IllegalArgumentException("event must contain six fields");
        return new Event(fromB64(fields[0]), fromB64(fields[1]), fromB64(fields[2]),
                Type.valueOf(fields[3]), Long.parseLong(fields[4]), Long.parseLong(fields[5]));
    }

    private static String b64(String value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String fromB64(String value) {
        return new String(Base64.getUrlDecoder().decode(value), StandardCharsets.UTF_8);
    }
}

