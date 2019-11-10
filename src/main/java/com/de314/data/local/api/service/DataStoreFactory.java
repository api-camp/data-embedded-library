package com.de314.data.local.api.service;

import com.de314.data.local.api.kv.KeyValueStore;
import com.de314.data.local.disk.RocksKeyValueStore;
import com.de314.data.local.memory.MapKeyValueStore;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.nio.file.FileSystems;
import java.util.function.Function;
import java.util.function.Supplier;

@Slf4j
public class DataStoreFactory {

    private static final DataStoreFactory DEFAULT_INSTANCE = new DataStoreFactory("./db");
    private static final DataStoreFactory TEST_INSTANCE = new DataStoreFactory("./db.test");

    private static final String SEPARATOR = FileSystems.getDefault().getSeparator();

    private final KeyValueStore<MapKeyValueStore> mapStoreCache = MapKeyValueStore.create();
    private final KeyValueStore<RocksKeyValueStore> rocksStoreCache = MapKeyValueStore.create();
    private final String rocksDbPathPrefix;

    public DataStoreFactory() {
        this("./db");
    }

    public DataStoreFactory(String rocksDbPathPrefix) {
        this.rocksDbPathPrefix = rocksDbPathPrefix;

        File dbDir = new File(rocksDbPathPrefix);
        if (!dbDir.isDirectory()) {
            if (!dbDir.exists()) {
                dbDir.mkdirs();
            } else {
                throw new RuntimeException(
                        "Wrapped",
                        new IllegalAccessException("Invalid rocks db path: " + rocksDbPathPrefix)
                );
            }
        }

    }

    public <V> MapKeyValueStore<V> getMemoryStore(String namespace) {
        return getStore(namespace, mapStoreCache, () -> MapKeyValueStore.create());
    }

    private String getRocksPath(String namespace) {
        return String.format("%s%s%s%sdb.rocks", rocksDbPathPrefix, SEPARATOR, namespace, SEPARATOR);
    }

    public final RocksKeyValueStore<JsonNode> getRocksStore(String namespace) {
        return getRocksStore(
                namespace,
                rocksDbPath -> {
                    try {
                        return RocksKeyValueStore.create(rocksDbPath);
                    } catch (RocksDBException e) {
                        e.printStackTrace();
                    }
                    return null;
                }
        );
    }

    public final <V> RocksKeyValueStore<V> getRocksStore(String namespace, Class<V> tagetClass) {
        return getRocksStore(
                namespace,
                rocksDbPath -> {
                    try {
                        return RocksKeyValueStore.create(rocksDbPath, tagetClass);
                    } catch (RocksDBException e) {
                        e.printStackTrace();
                    }
                    return null;
                }
        );
    }

    public final <V> RocksKeyValueStore<V> getRocksStore(String namespace, Function<String, RocksKeyValueStore<V>> storeCreator) {
        return getStore(namespace, rocksStoreCache, () -> {
            String rocksDbPath = getRocksPath(namespace);
            log.info("Connecting to Rocks DB at {}", rocksDbPath);
            return storeCreator.apply(rocksDbPath);
        });
    }

    public final void destroyRocksNamespace(String namespace) {
        synchronized (rocksStoreCache) {
            rocksStoreCache.get(namespace).ifPresent(store -> {
                // TODO: appears to work correctly
//                try {
//                    store.close();
//                } catch (Throwable t) {
//                    t.printStackTrace();
//                }
                String rocksPath = getRocksPath(namespace);
                File dbDir = new File(rocksPath);
                if (dbDir.exists() && dbDir.isDirectory()) {
                    deleteFolder(dbDir);
                }
            });
        }
    }

    private static void deleteFolder(File folder) {
        File[] files = folder.listFiles();
        if(files!=null) { //some JVMs return null for empty dirs
            for(File f: files) {
                if(f.isDirectory()) {
                    deleteFolder(f);
                } else {
                    f.delete();
                }
            }
        }
        folder.delete();
    }

    private final <StoreT extends KeyValueStore> StoreT getStore(String namespace, KeyValueStore<StoreT> storeCache, Supplier<StoreT> storeSupplier) {
        return storeCache.get(namespace).orElseGet(() -> {
                StoreT newStore = storeSupplier.get();
                storeCache.put(namespace, newStore);
                return newStore;
            });
    }

    public static DataStoreFactory instance() {
        return DEFAULT_INSTANCE;
    }

    public static DataStoreFactory testInstance() {
        return TEST_INSTANCE;
    }
}
