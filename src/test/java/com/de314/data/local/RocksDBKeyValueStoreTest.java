package com.de314.data.local;

import com.de314.data.local.api.KeyValueStore;
import com.de314.data.local.disk.RocksKeyValueStore;
import com.de314.data.local.model.Article;
import org.rocksdb.RocksDBException;

public class RocksDBKeyValueStoreTest extends AbstractKeyValueStoreTest {

    private static final RocksKeyValueStore ARTICLE_STORE;

    static {
        try {
            ARTICLE_STORE = RocksKeyValueStore.create("test.rocks", Article.class);
        } catch (RocksDBException e) {
            throw new RuntimeException("Could not create rocks connection", e);
        }
    }

    @Override
    KeyValueStore<Article> getStore() {
        return ARTICLE_STORE;
    }
}
