package com.de314.data.local.disk;

import com.de314.data.local.api.kv.AbstractKeyValueStore;
import com.de314.data.local.api.model.DataRow;
import com.de314.data.local.api.model.KVInfo;
import com.de314.data.local.api.model.ScanOptions;
import com.de314.data.local.api.service.ArchiveStrategy;
import com.de314.data.local.api.service.DataAdapter;
import com.de314.data.local.utils.FileUtils;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.File;
import java.util.Optional;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.de314.data.local.utils.FileUtils.SEPARATOR;

@Slf4j
public class RocksKeyValueStore<V> extends AbstractKeyValueStore<V> {

    public static final String STORE_KIND = "RocksDB";
    public static final String ROCKS_PATH = "./data";
    public static final String ROCKS_DB_PATH = ROCKS_PATH + "/db";
    public static final String ROCKS_ARCHIVE_PATH = ROCKS_PATH + "/archive";
    public static final String ROCKS_TMP_PATH = ROCKS_PATH + "/archive";

    static {
        new File(ROCKS_DB_PATH).mkdirs();
        new File(ROCKS_ARCHIVE_PATH).mkdirs();
        new File(ROCKS_TMP_PATH).mkdirs();
    }

    private final NamespaceOptions namespaceOptions;
    private final DataAdapter<V, byte[]> dataAdapter;
    private final DataAdapter<String, byte[]> keyAdapter;
    private final AtomicBoolean isOpen;

    private RocksDB _rocks;

    private RocksKeyValueStore(NamespaceOptions namespaceOptions, DataAdapter<V, byte[]> dataAdapter) {
        this.namespaceOptions = namespaceOptions;
        this.dataAdapter = dataAdapter;
        this.keyAdapter = DataAdapter.stringConverter();
        this.isOpen = new AtomicBoolean(false);
        connect();
    }

    private void connect() {
        Options rocksOptions = new Options();
        rocksOptions.setCreateIfMissing(true);
        rocksOptions.setErrorIfExists(false);
        try {
            this._rocks = RocksDB.open(rocksOptions, namespaceOptions.getRocksPath());
            isOpen.set(true);
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    private RocksDB rocks() {
        if (!isOpen.get()) {
            throw new RuntimeException(String.format("Database is closed: %s@%s", namespaceOptions.getNamespace(), STORE_KIND));
        }
        return _rocks;
    }

    @Override
    public KVInfo getInfo() {
        long size = FileUtils.directorySize(namespaceOptions.getPath());
        return KVInfo.builder()
                .kind(STORE_KIND)
                .namespace(namespaceOptions.getNamespace())
                .size(size)
                .prettySize(FileUtils.toPrettySize(size))
                .build();
    }

    @Override
    public long count() {
        long count = 0L;
        RocksIterator it = rocks().newIterator();
        it.seekToFirst();
        while (it.isValid()) {
            count++;
            it.next();
        }
        it.close();
        return count;
    }

    @Override
    public Optional<V> get(@NonNull String key) {
        try {
            return Optional.ofNullable(rocks().get(keyAdapter.ab(key)))
                    .map(dataAdapter.getBaFunc());
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }

    @Override
    public Stream<DataRow<V>> stream(ScanOptions options) {
        String startKey = options.getStartKey();
        RocksIterator it = rocks().newIterator();

        if (startKey != null) {
            it.seek(keyAdapter.ab(startKey));
        } else {
            it.seekToFirst();
        }

        if (!it.isValid()) {
            it.close();
            return Stream.empty();
        }

        AtomicLong count = new AtomicLong();
        long limit = options.getLimit(Long.MAX_VALUE);
        boolean keysOnly = options.getKeysOnly(false);

        return StreamSupport.stream(
                new Spliterator<DataRow<V>>() {
                    @Override
                    public boolean tryAdvance(Consumer<? super DataRow<V>> action) {
                        boolean hasMore = false;
                        if (it.isValid()) {
                            hasMore = false;
                            String key = keyAdapter.ba(it.key());
                            if (options.keyInRange(key) && count.getAndIncrement() < limit) {
                                V value = keysOnly ? null : dataAdapter.ba(it.value());
                                action.accept(DataRow.of(key, value));
                                it.next();
                                hasMore = it.isValid();
                            }
                        } else {
                            it.close();
                        }
                        return hasMore;
                    }

                    @Override
                    public Spliterator<DataRow<V>> trySplit() {
                        return null;
                    }

                    @Override
                    public long estimateSize() {
                        return 0;
                    }

                    @Override
                    public int characteristics() {
                        return Spliterator.ORDERED;
                    }
                },
                false
        );
    }

    @Override
    public void put(@NonNull String key, V value) {
        try {
            rocks().put(keyAdapter.ab(key), dataAdapter.ab(value));
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean delete(@NonNull String key) {
        byte[] value = null;
        try {
            byte[] rocksKey = keyAdapter.ab(key);
            value = rocks().get(rocksKey);
            rocks().delete(keyAdapter.ab(key));
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return value != null && value.length > 0;
    }

    @Override
    public long delete(ScanOptions options) {
        long result = super.delete(options);
        try {
            rocks().compactRange();
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return result;
    }

    @Override
    public void close() {
        try {
            RocksDB rocks = rocks();
            isOpen.set(false);
            // TODO: count down latch???
            FlushOptions options = new FlushOptions();
            options.setWaitForFlush(true);
            rocks.flush(options);
            rocks.close();
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    public void destroy() {
        try {
            this.close();
        } catch (Throwable t) {
            t.printStackTrace();
        }
        boolean deleted = FileUtils.deleteFolder(namespaceOptions.getPath());
        log.info("Destroying {} => {}", namespaceOptions.getNamespace(), deleted);
    }

    public boolean backup(ArchiveStrategy archiveStrategy) {
        String namespace = namespaceOptions.getNamespace();
        String archiveFilename = new StringBuilder(ROCKS_ARCHIVE_PATH)
                .append(SEPARATOR)
                .append(namespace)
                .append("-")
                .append(System.currentTimeMillis())
                .append(".zip")
                .toString();
        if (FileUtils.zipDir(namespaceOptions.getPath(), archiveFilename)) {
            return archiveStrategy.offload(namespace, archiveFilename);
        }
        return false;
    }

    public boolean rollback(ArchiveStrategy archiveStrategy) {
        this.destroy();
        return archiveStrategy.reload(namespaceOptions.getNamespace());
    }

    public static RocksKeyValueStore<JsonNode> create(String namespace) {
        return create(namespace, DataAdapter.jsonByteAdapter());
    }

    public static <ValueT> RocksKeyValueStore<ValueT> create(String namespace, Class<ValueT> valueClass) {
        return create(namespace, DataAdapter.pojoByteConverter(valueClass));
    }

    public static <ValueT> RocksKeyValueStore<ValueT> create(String namespace, DataAdapter<ValueT, byte[]> dataAdapter) {

        StringBuilder pathBuilder = new StringBuilder(ROCKS_DB_PATH).append(SEPARATOR).append(namespace);
        String path = pathBuilder.toString();
        String rocksPath = pathBuilder.append(SEPARATOR).append("db.rocks").toString();
        NamespaceOptions namespaceOptions = NamespaceOptions.builder()
                .namespace(namespace)
                .path(path)
                .rocksPath(rocksPath)
                .build();

        return create(namespaceOptions, dataAdapter);
    }

    private static <ValueT> RocksKeyValueStore<ValueT> create(NamespaceOptions options, DataAdapter<ValueT, byte[]> dataAdapter) {
        File dbDir = new File(options.getRocksPath());
        if (!dbDir.exists()) {
            dbDir.mkdirs();
        } else if (!dbDir.isDirectory()) {
            throw new RuntimeException(
                    "Wrapped",
                    new IllegalAccessException("Invalid rocks db path: " + options)
            );
        }
        Options rocksOptions = new Options();
        rocksOptions.setCreateIfMissing(true);
        rocksOptions.setErrorIfExists(false);
        return new RocksKeyValueStore<>(options, dataAdapter);
    }

    @Data
    @Builder
    public static class NamespaceOptions {
        private final String namespace;
        private final String path;
        private final String rocksPath;
    }
}
