package io.github.de314.ac.data.api.kv;

import io.github.de314.ac.data.api.model.CursorPage;
import io.github.de314.ac.data.api.model.DataRow;
import io.github.de314.ac.data.api.model.ScanOptions;

import java.util.List;
import java.util.stream.Collectors;

public abstract class AbstractKeyValueStore<V> implements KeyValueStore<V> {

    @Override
    public long count(ScanOptions options) {
        return stream(options.toBuilder().keysOnly(true).build()).collect(Collectors.counting());
    }

    @Override
    public CursorPage<V> scan(ScanOptions options) {
        long cursorLimit = options.getLimit(100L) + 1;
        List<DataRow<V>> content = stream(options.toBuilder().limit(cursorLimit).build())
                .collect(Collectors.toList());

        ScanOptions next = null;
        if (content.size() == cursorLimit) {
            DataRow<V> lastRecord = content.get(content.size() - 1);
            next = options.toBuilder()
                    .startKey(lastRecord.getKey())
                    .build();
            content = content.subList(0, content.size() - 1);
        }

        return new CursorPage<>(content, next);
    }

    @Override
    public long delete(ScanOptions options) {
        long count = 0L;
        do {
            CursorPage<V> page = scan(options.toBuilder().limit(200L).keysOnly(true).build());
            for (DataRow<V> row : page.getContent()) {
                count++;
                this.delete(row.getKey());
            }
            options = page.getNext();
        } while (options != null);
        return count;
    }
}
