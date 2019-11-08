package com.de314.data.local.api;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

@Data
@Builder(toBuilder = true)
public class ScanOptions {

    private final String startKey;
    private final String endKey;
    private final Long limit;
    private final Boolean keysOnly;

    public boolean keyInRange(String key) {
        return (startKey == null || startKey.compareTo(key) <= 0)
                && (endKey == null || endKey.compareTo(key) >= 0);
    }

    public long getLimit(long defaultValue) {
        return limit != null ? limit : defaultValue;
    }

    public boolean getKeysOnly(boolean defaultValue) {
        return keysOnly != null ? keysOnly : defaultValue;
    }

    public static ScanOptions.ScanOptionsBuilder all() {
        return fromCursor(null);
    }

    public static ScanOptions.ScanOptionsBuilder fromCursor(String cursor) {
        return fromRange(cursor, null);
    }

    public static ScanOptions.ScanOptionsBuilder fromPrefix(@NonNull String prefix) {
        return fromPrefix(prefix, null);
    }

    public static ScanOptions.ScanOptionsBuilder fromPrefix(@NonNull String prefix, String cursor) {
        return fromRange(
                cursor != null ? cursor : prefix,
                prefix + ((char)Short.MAX_VALUE)
        );
    }

    public static ScanOptions.ScanOptionsBuilder fromRange(String startKey, String endKey) {
        return ScanOptions.builder()
                .startKey(startKey)
                .endKey(endKey);
    }
}
