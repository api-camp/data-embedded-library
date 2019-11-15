package com.de314.data.local.utils;

import java.util.Spliterator;

public abstract class BaseSpliterator<T> implements Spliterator<T> {

    @Override
    public Spliterator<T> trySplit() {
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
}
