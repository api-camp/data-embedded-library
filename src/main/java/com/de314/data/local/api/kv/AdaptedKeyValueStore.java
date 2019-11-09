package com.de314.data.local.api.kv;

import com.de314.data.local.api.model.CursorPage;
import com.de314.data.local.api.service.DataAdapter;
import com.de314.data.local.api.model.DataRow;
import com.de314.data.local.api.model.ScanOptions;
import lombok.AllArgsConstructor;
import lombok.NonNull;

import java.util.Optional;
import java.util.stream.Stream;

@AllArgsConstructor
public class AdaptedKeyValueStore<A, B> implements KeyValueStore<A> {

    private final KeyValueStore<B> delegate;
    private final DataAdapter<A, B> dataAdapter;

    @Override
    public long count() {
        return delegate.count();
    }

    @Override
    public long count(ScanOptions options) {
        return delegate.count(options);
    }

    @Override
    public Optional<A> get(@NonNull String key) {
        return delegate.get(key).map(dataAdapter.getBaFunc());
    }

    @Override
    public CursorPage<A> scan(ScanOptions options) {
        return delegate.scan(options).map(dataAdapter.getBaFunc());
    }

    @Override
    public Stream<DataRow<A>> stream(ScanOptions options) {
        return delegate.stream(options).map(
                row -> DataRow.of(row.getKey(), dataAdapter.ba(row.getValue()))
        );
    }

    @Override
    public void put(@NonNull String key, A value) {
        delegate.put(key, dataAdapter.ab(value));
    }

    @Override
    public boolean delete(@NonNull String key) {
        return delegate.delete(key);
    }

    @Override
    public long delete(ScanOptions options) {
        return delegate.delete(options);
    }

    @Override
    public void close() {
        delegate.close();
    }
}
