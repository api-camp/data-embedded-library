package com.de314.data.local.api;

import lombok.NonNull;

import java.util.Optional;
import java.util.stream.Stream;

public interface KeyValueStore<V> {

    long count();

    long count(ScanOptions options);

    Optional<V> get(@NonNull String key);

    CursorPage<V> scan(ScanOptions options);

    Stream<DataRow<V>> stream(ScanOptions options);

    void put(@NonNull String key, V value);

    boolean delete(@NonNull String key);

    long delete(ScanOptions options);
}
