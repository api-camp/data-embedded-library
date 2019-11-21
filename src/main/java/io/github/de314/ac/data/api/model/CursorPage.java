package io.github.de314.ac.data.api.model;

import lombok.Data;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

@Data
public class CursorPage<V> {

    private final List<DataRow<V>> content;
    private final ScanOptions next;

    public <B> CursorPage<B> map(Function<V, B> dataMapper) {
        return new CursorPage<>(
                content.stream()
                        .map(row -> DataRow.of(row.getKey(), dataMapper.apply(row.getValue())))
                        .collect(Collectors.toList()),
                next
        );
    }
}
