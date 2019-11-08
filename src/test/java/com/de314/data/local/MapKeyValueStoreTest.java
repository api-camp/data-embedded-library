package com.de314.data.local;

import com.de314.data.local.api.KeyValueStore;
import com.de314.data.local.memory.MapKeyValueStore;
import com.de314.data.local.model.Article;
import org.junit.jupiter.api.Test;

public class MapKeyValueStoreTest extends AbstractKeyValueStoreTest {

    @Override
    KeyValueStore<Article> getStore() {
        return MapKeyValueStore.create();
    }

    @Test
    void test() {

    }
}