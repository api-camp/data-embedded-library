package io.github.de314.ac.data.api.service;

import io.github.de314.ac.data.api.kv.KeyValueStore;
import io.github.de314.ac.data.api.model.KVInfo;
import io.github.de314.ac.data.api.model.ScanOptions;
import io.github.de314.ac.data.memory.MapKeyValueStore;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public abstract class AbstractDataStoreService implements DataStoreService {

    @Getter
    private final String storeKind;

    @Getter(AccessLevel.PROTECTED)
    private final KeyValueStore<StoreEntry> storeCache;

    public AbstractDataStoreService(String storeKind) {
        this.storeKind = storeKind;
        this.storeCache = MapKeyValueStore.create("__store-service-" + storeKind);
    }

    @Override
    public List<KVInfo> entries() {
        return storeCache.stream(ScanOptions.all().build())
                .map(row -> row.getValue().getStore().getInfo())
                .collect(Collectors.toList());
    }

    @Override
    public Optional<KVInfo> getInfo(String namespace) {
        return storeCache.get(namespace)
                .map(storeEntry -> storeEntry.getStore().getInfo());
    }

    @Override
    public Optional<KeyValueStore<JsonNode>> get(String namespace) {
        return get(namespace, JsonNode.class);
    }

    abstract protected KeyValueStore<JsonNode> create(String namespace);

    @Override
    public KeyValueStore<JsonNode> getOrCreate(String namespace) {
        return get(namespace)
                .orElseGet(() -> {
                    KeyValueStore<JsonNode> store = create(namespace);
                    storeCache.put(namespace, StoreEntry.builder()
                            .namespace(namespace)
                            .store(store)
                            .valueKind(JsonNode.class)
                            .build());
                    return store;
                });
    }

    @Override
    public <T> Optional<KeyValueStore<T>> get(String namespace, Class<T> modelClass) {
        return storeCache.get(namespace)
                .flatMap(storeEntry -> storeEntry.as(modelClass));
    }

    abstract protected <ValueT> KeyValueStore<ValueT> create(String namespace, Class<ValueT> modelClass);

    @Override
    public <ValueT> KeyValueStore<ValueT> getOrCreate(String namespace, Class<ValueT> modelClass) {
        return get(namespace, modelClass)
                .orElseGet(() -> {
                    KeyValueStore<ValueT> store = create(namespace, modelClass);
                    storeCache.put(namespace, StoreEntry.builder()
                            .namespace(namespace)
                            .store(store)
                            .valueKind(JsonNode.class)
                            .build());
                    return store;
                });
    }

    @Override
    public void backup(String namespace, ArchiveStrategy archiveStrategy) {
        // no-op
        log.warn("Backup not implemented for {} => {}", getClass().getCanonicalName(), getStoreKind());
    }

    @Override
    public void rollback(String namespace, ArchiveStrategy archiveStrategy) {
        // no-op
        log.warn("Rollback not implemented for {} => {}", getClass().getCanonicalName(), getStoreKind());
    }

    @Override
    public void destroy(String namespace) {
        storeCache.delete(namespace);
    }

    @Data
    @Builder
    protected static class StoreEntry {

        private final String namespace;
        private final KeyValueStore store;
        private final Class<?> valueKind;

        public <ValueT> Optional<KeyValueStore<ValueT>> as(Class<ValueT> targetKind) {
            return Optional.of(store)
                    .filter(s -> targetKind.isAssignableFrom(valueKind))
                    .map(s -> (KeyValueStore<ValueT>) s);
        }

        public static <ValueT> StoreEntry of(String namespace, KeyValueStore<ValueT> store, Class<ValueT> valueKind) {
            return StoreEntry.builder()
                    .namespace(namespace)
                    .store(store)
                    .valueKind(valueKind)
                    .build();
        }
    }
}
