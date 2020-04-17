package io.github.de314.ac.data.disk;

import io.github.de314.ac.data.api.model.KVInfo;
import io.github.de314.ac.data.api.model.NamespaceOptions;
import io.github.de314.ac.data.utils.FileUtils;
import io.github.de314.ac.data.utils.UncheckedException;
import io.github.de314.ac.data.utils.metrics.Timer;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class RocksDbService {

    public static final String STORE_KIND = "RocksDB";
    public static final int MAX_CLOSE_WAIT_TIME_MS = 3_000;


    /*
    public static final String ROCKS_ARCHIVE_PATH = ROCKS_PATH + "/archive";
    public static final String ROCKS_TMP_PATH = ROCKS_PATH + "/archive";
    public static final int MAX_CLOSE_WAIT_TIME_MS = 3_000;
        new File(ROCKS_DB_PATH).mkdirs();
        new File(ROCKS_ARCHIVE_PATH).mkdirs();
        new File(ROCKS_TMP_PATH).mkdirs();
     */

    static {
        RocksDB.loadLibrary();
    }

    private final NamespaceOptions namespaceOptions;
    private final AtomicBoolean isOpen;
    private final AtomicInteger countDownLatch;

    private final RocksDB _rocks;

    private RocksDbService(NamespaceOptions namespaceOptions) {
        this.namespaceOptions = namespaceOptions;
        this.isOpen = new AtomicBoolean(false);
        this.countDownLatch = new AtomicInteger();

        Options rocksOptions = new Options();
        rocksOptions.setCreateIfMissing(true);
        rocksOptions.setErrorIfExists(false);
        this._rocks = UncheckedException.safe(() -> {
            RocksDB rocks = RocksDB.open(rocksOptions, namespaceOptions.getRocksPath());
            isOpen.set(true);
            return rocks;
        }).orElse(null);
    }

    private RocksDB getRocks() {
        if (!isOpen.get()) {
            throw new RuntimeException(String.format("Database is closed: %s@%s", namespaceOptions.getNamespace(), STORE_KIND));
        }
        return _rocks;
    }

    public RocksIterator getIterator() {
        RocksIterator it = getRocks().newIterator();
        countDownLatch.getAndIncrement();
        return it;
    }

    public void closeIterator(RocksIterator it) {
        it.close();
        countDownLatch.getAndDecrement();
    }

    public KVInfo getInfo() {
        long size = FileUtils.directorySize(namespaceOptions.getPath());
        return KVInfo.builder()
                .kind(STORE_KIND)
                .namespace(namespaceOptions.getNamespace())
                .size(size)
                .prettySize(FileUtils.toPrettySize(size))
                .build();
    }

    public long countAll() {
        long count = 0L;
        RocksIterator it = getIterator();
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

    public byte[] get(@NonNull byte[] key) {
        return UncheckedException.safe(() -> getRocks().get(key))
                .orElseGet(() -> new byte[0]);
    }

    public void put(@NonNull byte[] key, byte[] value) {
        UncheckedException.safe(() -> getRocks().put(key, value));
    }

    public boolean delete(@NonNull byte[] key) {
        return UncheckedException.safe(() -> {
            byte[] value = getRocks().get(key);
            getRocks().delete(key);
            return value;
        }).isPresent();
    }

    public void compact() {
        UncheckedException.safe(() -> getRocks().compactRange());
    }

    public void close() {
        try {
            RocksDB rocks = getRocks();
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

//    public boolean backup(ArchiveStrategy archiveStrategy) {
//        String namespace = namespaceOptions.getNamespace();
//        String archiveFilename = new StringBuilder(ROCKS_ARCHIVE_PATH)
//                .append(FileUtils.SEPARATOR)
//                .append(namespace)
//                .append("-")
//                .append(System.currentTimeMillis())
//                .append(".zip")
//                .toString();
//        if (FileUtils.zipDir(namespaceOptions.getPath(), archiveFilename)) {
//            return archiveStrategy.offload(namespace, archiveFilename);
//        }
//        return false;
//    }
//
//    public boolean rollback(ArchiveStrategy archiveStrategy) {
//        // close and delete everything
//        this.destroy();
//        // download and replace db
//        boolean success = archiveStrategy.reload(namespaceOptions.getNamespace());
//        if (success) {
//            // start up db upon success
//            connect();
//        }
//        return success && isOpen.get();
//    }

    public static RocksDbService create(NamespaceOptions options) {
        File dbDir = new File(options.getRocksPath());
        if (!dbDir.exists()) {
            dbDir.mkdirs();
        }
        if (!dbDir.isDirectory()) {
            UncheckedException.of(
                    new IllegalAccessException("Invalid rocks db path: " + options)
            );
        }
        Options rocksOptions = new Options();
        rocksOptions.setCreateIfMissing(true);
        rocksOptions.setErrorIfExists(false);
        return new RocksDbService(options);
    }
}
