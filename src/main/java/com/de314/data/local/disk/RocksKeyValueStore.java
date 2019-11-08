package com.de314.data.local.disk;

import com.de314.data.local.api.AbstractKeyValueStore;
import com.de314.data.local.api.DataAdapter;
import com.de314.data.local.api.DataRow;
import com.de314.data.local.api.ScanOptions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.util.Optional;
import java.util.Spliterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class RocksKeyValueStore<V> extends AbstractKeyValueStore<V> {

    private final RocksDB rocks;
    private final DataAdapter<V, byte[]> dataAdapter;
    private final DataAdapter<String, byte[]> keyAdapter = DataAdapter.stringConverter();

    @Override
    public long count() {
        long count = 0L;
        RocksIterator it = rocks.newIterator();
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
            return Optional.ofNullable(rocks.get(keyAdapter.ab(key)))
                    .map(dataAdapter.getBaFunc());
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return Optional.empty();
    }

    @Override
    public Stream<DataRow<V>> stream(ScanOptions options) {
        String startKey = options.getStartKey();
        RocksIterator it = rocks.newIterator();

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
            rocks.put(keyAdapter.ab(key), dataAdapter.ab(value));
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean delete(@NonNull String key) {
        byte[] value = null;
        try {
            byte[] rocksKey = keyAdapter.ab(key);
            value = rocks.get(rocksKey);
            rocks.delete(keyAdapter.ab(key));
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
        return value != null && value.length > 0;
    }

    public static <ValueT> RocksKeyValueStore<ValueT> create(String rocksPath, Class<ValueT> valueClass) throws RocksDBException {
        return create(rocksPath, DataAdapter.pojoByteConverter(valueClass));
    }

    public static <ValueT> RocksKeyValueStore<ValueT> create(String rocksPath, DataAdapter<ValueT, byte[]> dataAdapter) throws RocksDBException {
        RocksDB rocks = RocksDB.open(rocksPath);
        return new RocksKeyValueStore<>(rocks, dataAdapter);
    }
}
