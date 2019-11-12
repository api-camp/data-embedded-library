package com.de314.data.local.memory;

import com.de314.data.local.api.kv.AbstractKeyValueStore;
import com.de314.data.local.api.model.DataRow;
import com.de314.data.local.api.model.KVInfo;
import com.de314.data.local.api.model.ScanOptions;
import com.google.common.collect.Maps;
import lombok.NonNull;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public class MapKeyValueStore<V> extends AbstractKeyValueStore<V> {

    public static final String STORE_KIND = "MemoryMap";

    private final String namespace;
    private final Map<String, V> store = Maps.newTreeMap();

    public MapKeyValueStore(String namespace) {
        this.namespace = namespace;
    }

    @Override
    public KVInfo getInfo() {
        int size = store.size();
        return KVInfo.builder()
                .kind(STORE_KIND)
                .namespace(namespace)
                .size(size)
                .prettySize(String.format("%,d records", size))
                .build();
    }

    @Override
    public long count() {
        return store.size();
    }

    @Override
    public Optional<V> get(@NonNull String key) {
        return Optional.ofNullable(store.get(key));
    }

    @Override
    public Stream<DataRow<V>> stream(ScanOptions options) {
        long limit = options.getLimit(100L);
        boolean keysOnly = options.getKeysOnly(false);
        return store.keySet().stream()
                .filter(options::keyInRange)
                .limit(limit)
                .map(key -> DataRow.of(
                        key,
                        keysOnly ? null : store.get(key)
                ));
    }

    @Override
    public void put(@NonNull String key, V value) {
        store.put(key, value);
    }

    @Override
    public boolean delete(@NonNull String key) {
        return store.remove(key) != null;
    }

    @Override
    public void close() {
        // ignored
    }

    public static <ValueT> MapKeyValueStore<ValueT> create(String namespace) {
        return new MapKeyValueStore<>(namespace);
    }
}
