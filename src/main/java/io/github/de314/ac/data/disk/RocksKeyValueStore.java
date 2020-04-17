package io.github.de314.ac.data.disk;

import io.github.de314.ac.data.api.Constants;
import io.github.de314.ac.data.api.kv.AbstractKeyValueStore;
import io.github.de314.ac.data.api.model.DataRow;
import io.github.de314.ac.data.api.model.KVInfo;
import io.github.de314.ac.data.api.model.NamespaceOptions;
import io.github.de314.ac.data.api.model.ScanOptions;
import io.github.de314.ac.data.api.service.DataAdapter;
import io.github.de314.ac.data.utils.BaseSpliterator;
import io.github.de314.ac.data.utils.FileUtils;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Slf4j
public class RocksKeyValueStore extends AbstractKeyValueStore<byte[]> {

    public static final String STORE_KIND = "RocksDB";

    static {
        RocksDB.loadLibrary();
    }

    private final DataAdapter<String, byte[]> keyAdapter;
    private final RocksDbService rocksService;

    private final NamespaceOptions namespaceOptions;

    private RocksKeyValueStore(RocksDbService rocksService, NamespaceOptions namespaceOptions) {
        this.rocksService = rocksService;
        this.namespaceOptions = namespaceOptions;
        this.keyAdapter = DataAdapter.stringConverter();
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
        RocksIterator it = rocksService.getIterator();
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
            rocksService.closeIterator(it);
        }
        return count;
    }

    @Override
    public Optional<byte[]> get(@NonNull String key) {
        return Optional.ofNullable(rocksService.get(keyAdapter.ab(key)));
    }

    @Override
    public Stream<DataRow<byte[]>> stream(ScanOptions options) {
        final String startKey = options.getStartKey();

        final RocksIterator it = rocksService.getIterator();
        log.trace("Obtained stream iterator {}", it.hashCode());

        if (startKey != null) {
            it.seek(keyAdapter.ab(startKey));
        } else {
            it.seekToFirst();
        }

        if (!it.isValid()) {
            log.trace("Closing stream iterator {}", it.hashCode());
            rocksService.closeIterator(it);
            return Stream.empty();
        }

        AtomicLong count = new AtomicLong();
        long limit = options.getLimit(Long.MAX_VALUE);
        boolean keysOnly = options.getKeysOnly(false);

        return StreamSupport.stream(
                new BaseSpliterator<DataRow<byte[]>>() {
                    @Override
                    public boolean tryAdvance(Consumer<? super DataRow<byte[]>> action) {
                        boolean hasMore = it.isValid();
                        if (hasMore) {
                            hasMore = false;
                            String key = keyAdapter.ba(it.key());
                            if (options.keyInRange(key) && count.getAndIncrement() < limit) {
                                byte[] value = keysOnly ? null : it.value();
                                action.accept(DataRow.of(key, value));
                                it.next();
                                hasMore = it.isValid();
                            }
                        }
                        if (!hasMore) {
                            log.trace("Closing stream iterator {}", it.hashCode());
                            rocksService.closeIterator(it);
                        }
                        return hasMore;
                    }
                },
                false
        );
    }

    @Override
    public void put(@NonNull String key, byte[] value) {
        rocksService.put(keyAdapter.ab(key), value);
    }

    @Override
    public boolean delete(@NonNull String key) {
        return rocksService.delete(keyAdapter.ab(key));
    }

    @Override
    public long delete(ScanOptions options) {
        long result = super.delete(options);
        rocksService.compact();
        return result;
    }

    @Override
    public void close() {
        rocksService.close();
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

    public static RocksKeyValueStore create(String namespace) {
        return create(NamespaceOptions.create(namespace));
    }

    public static RocksKeyValueStore create(NamespaceOptions options) {
        return new RocksKeyValueStore(
                RocksDbService.create(options),
                options
        );
    }
}
