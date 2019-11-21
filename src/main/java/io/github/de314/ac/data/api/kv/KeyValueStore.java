package io.github.de314.ac.data.api.kv;

import io.github.de314.ac.data.api.model.CursorPage;
import io.github.de314.ac.data.api.model.DataRow;
import io.github.de314.ac.data.api.model.KVInfo;
import io.github.de314.ac.data.api.model.ScanOptions;
import lombok.NonNull;

import java.util.Optional;
import java.util.stream.Stream;

public interface KeyValueStore<V> {

    KVInfo getInfo();

    long count();

    long count(ScanOptions options);

    Optional<V> get(@NonNull String key);

    CursorPage<V> scan(ScanOptions options);

    Stream<DataRow<V>> stream(ScanOptions options);

    void put(@NonNull String key, V value);

    boolean delete(@NonNull String key);

    long delete(ScanOptions options);

    void close();
}
