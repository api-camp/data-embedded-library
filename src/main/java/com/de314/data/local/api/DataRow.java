package com.de314.data.local.api;

import lombok.Data;
import lombok.NonNull;

@Data
public class DataRow<V> {

    private final String key;
    private final V value;

    public static <ValueT> DataRow<ValueT> of(@NonNull String key, ValueT value) {
        return new DataRow<>(key, value);
    }
}
