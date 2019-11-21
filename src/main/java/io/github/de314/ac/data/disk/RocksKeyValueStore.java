package io.github.de314.ac.data.disk;

import io.github.de314.ac.data.api.Constants;
import io.github.de314.ac.data.api.kv.AbstractKeyValueStore;
import io.github.de314.ac.data.api.model.DataRow;
import io.github.de314.ac.data.api.model.KVInfo;
import io.github.de314.ac.data.api.model.ScanOptions;
import io.github.de314.ac.data.api.service.ArchiveStrategy;
import io.github.de314.ac.data.api.service.DataAdapter;
import io.github.de314.ac.data.utils.BaseSpliterator;
import io.github.de314.ac.data.utils.FileUtils;
import io.github.de314.ac.data.utils.metrics.Timer;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.File;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Slf4j
public class RocksKeyValueStore<V> extends AbstractKeyValueStore<V> {

    public static final String STORE_KIND = "RocksDB";
    public static final String ROCKS_PATH = Constants.DATA_DIRECTORY_PATH;
    public static final String ROCKS_DB_PATH = ROCKS_PATH + "/db";
    public static final String ROCKS_ARCHIVE_PATH = ROCKS_PATH + "/archive";
    public static final String ROCKS_TMP_PATH = ROCKS_PATH + "/archive";
    public static final int MAX_CLOSE_WAIT_TIME_MS = 3_000;

    static {
        RocksDB.loadLibrary();
        new File(ROCKS_DB_PATH).mkdirs();
        new File(ROCKS_ARCHIVE_PATH).mkdirs();
        new File(ROCKS_TMP_PATH).mkdirs();
    }

    private final NamespaceOptions namespaceOptions;
    private final DataAdapter<V, byte[]> dataAdapter;
    private final DataAdapter<String, byte[]> keyAdapter;
    private final AtomicBoolean isOpen;
    private final AtomicInteger countDownLatch;

    private RocksDB _rocks;

    private RocksKeyValueStore(NamespaceOptions namespaceOptions, DataAdapter<V, byte[]> dataAdapter) {
        this.namespaceOptions = namespaceOptions;
        this.dataAdapter = dataAdapter;
        this.keyAdapter = DataAdapter.stringConverter();
        this.isOpen = new AtomicBoolean(false);
        this.countDownLatch = new AtomicInteger();
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

    private RocksIterator rocksIterator() {
        RocksIterator it = rocks().newIterator();
        countDownLatch.getAndIncrement();
        return it;
    }

    private void closeIterator(RocksIterator it) {
        it.close();
        countDownLatch.getAndDecrement();
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
        RocksIterator it = rocksIterator();
        log.trace("Obtained count iterator {}", it.hashCode());
        try {
            it.seekToFirst();
            while (it.isValid()) {
                count++;
                it.next();
            }
        } catch (Throwable t) {
            log.error("Failed count all", t);
        } finally {
            log.trace("Closing count iterator {}", it.hashCode());
            closeIterator(it);
        }
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
        final String startKey = options.getStartKey();

        final RocksIterator it = rocksIterator();
        log.trace("Obtained stream iterator {}", it.hashCode());

        if (startKey != null) {
            it.seek(keyAdapter.ab(startKey));
        } else {
            it.seekToFirst();
        }

        if (!it.isValid()) {
            log.trace("Closing stream iterator {}", it.hashCode());
            closeIterator(it);
            return Stream.empty();
        }

        AtomicLong count = new AtomicLong();
        long limit = options.getLimit(Long.MAX_VALUE);
        boolean keysOnly = options.getKeysOnly(false);

        return StreamSupport.stream(
                new BaseSpliterator<DataRow<V>>() {
                    @Override
                    public boolean tryAdvance(Consumer<? super DataRow<V>> action) {
                        boolean hasMore = it.isValid();
                        if (hasMore) {
                            hasMore = false;
                            String key = keyAdapter.ba(it.key());
                            if (options.keyInRange(key) && count.getAndIncrement() < limit) {
                                V value = keysOnly ? null : dataAdapter.ba(it.value());
                                action.accept(DataRow.of(key, value));
                                it.next();
                                hasMore = it.isValid();
                            }
                        }
                        if (!hasMore) {
                            log.trace("Closing stream iterator {}", it.hashCode());
                            closeIterator(it);
                        }
                        return hasMore;
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

            Timer timer = Timer.create();
            while (countDownLatch.get() > 0 && timer.getDuration() < MAX_CLOSE_WAIT_TIME_MS) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (countDownLatch.get() > 0) {
                throw new RuntimeException("Could not close database. " + countDownLatch.get() + " open iterators");
            }

            FlushOptions options = new FlushOptions();
            options.setWaitForFlush(true);
            rocks.flush(options);
            ColumnFamilyHandle cf = rocks.getDefaultColumnFamily();
            cf.close();
            rocks.close();
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    public void destroy() {
        this.close();
        boolean deleted = FileUtils.delete(namespaceOptions.getPath());
        log.info("Destroyed {} => {} :: {}", namespaceOptions.getNamespace(), deleted, namespaceOptions.getPath());
    }

    public boolean backup(ArchiveStrategy archiveStrategy) {
        String namespace = namespaceOptions.getNamespace();
        String archiveFilename = new StringBuilder(ROCKS_ARCHIVE_PATH)
                .append(FileUtils.SEPARATOR)
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
        // close and delete everything
        this.destroy();
        // download and replace db
        boolean success = archiveStrategy.reload(namespaceOptions.getNamespace());
        if (success) {
            // start up db upon success
            connect();
        }
        return success && isOpen.get();
    }

    public static RocksKeyValueStore<JsonNode> create(String namespace) {
        return create(namespace, DataAdapter.jsonByteAdapter());
    }

    public static <ValueT> RocksKeyValueStore<ValueT> create(String namespace, Class<ValueT> valueClass) {
        return create(namespace, DataAdapter.pojoByteConverter(valueClass));
    }

    public static <ValueT> RocksKeyValueStore<ValueT> create(String namespace, DataAdapter<ValueT, byte[]> dataAdapter) {

        StringBuilder pathBuilder = new StringBuilder(ROCKS_DB_PATH).append(FileUtils.SEPARATOR).append(namespace);
        String path = pathBuilder.toString();
        String rocksPath = pathBuilder.append(FileUtils.SEPARATOR).append("db.rocks").toString();
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
