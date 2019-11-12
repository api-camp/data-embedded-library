package com.de314.data.local.disk;

import com.de314.data.local.api.kv.KeyValueStore;
import com.de314.data.local.api.service.AbstractDataStoreService;
import com.de314.data.local.api.service.ArchiveStrategy;
import com.de314.data.local.api.service.DataStoreService;
import com.de314.data.local.utils.metrics.Timer;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;

import java.util.Optional;
import java.util.function.Consumer;

@Slf4j
public class RockDBDataStoreService extends AbstractDataStoreService {

    public static final RockDBDataStoreService INSTANCE = new RockDBDataStoreService();

    public RockDBDataStoreService() {
        super(RocksKeyValueStore.STORE_KIND);
    }

    @Override
    protected KeyValueStore<JsonNode> create(String namespace) {
        return RocksKeyValueStore.create(namespace);
    }

    @Override
    protected <ValueT> KeyValueStore<ValueT> create(String namespace, Class<ValueT> modelClass) {
        return RocksKeyValueStore.create(namespace, modelClass);
    }

    @Override
    public void backup(String namespace, ArchiveStrategy archiveStrategy) {
        useStore(namespace, rocksStore -> {
            log.info("Backing up: {} with strategy={}", namespace, archiveStrategy.getName());
            Timer timer = Timer.create();
            boolean success = rocksStore.backup(archiveStrategy);
            timer.stop();
            log.info("Backup Complete [{}]: {} {}", success ? "SUCCESS" : "FAILED", namespace, timer);
        });
    }

    @Override
    public void rollback(String namespace, ArchiveStrategy archiveStrategy) {
        useStore(namespace, rocksStore -> {
            log.info("Rolling back: {} with strategy={}", namespace, archiveStrategy.getName());
            Timer timer = Timer.create();
            boolean success = rocksStore.rollback(archiveStrategy);
            timer.stop();
            log.info("Roll back complete [{}]: {} {}", success ? "SUCCESS" : "FAILED", namespace, timer);
        });
    }

    @Override
    public void destroy(String namespace) {
        useStore(namespace, rocksStore -> {
            rocksStore.destroy();
            getStoreCache().delete(namespace);
        });
    }

    private void useStore(String namespace, Consumer<RocksKeyValueStore> job) {
        Optional<StoreEntry> storeEntry = getStoreCache().get(namespace);
        if (storeEntry.isPresent()) {
            RocksKeyValueStore rocksStore = (RocksKeyValueStore) storeEntry.get().getStore();
            synchronized (rocksStore) {
                job.accept(rocksStore);
            }
        }
    }

    public static DataStoreService instance() {
        return INSTANCE;
    }
}
