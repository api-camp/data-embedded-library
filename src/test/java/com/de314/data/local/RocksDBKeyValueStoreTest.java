package com.de314.data.local;

import com.de314.data.local.api.kv.KeyValueStore;
import com.de314.data.local.api.service.DataStoreFactory;
import com.de314.data.local.disk.RocksKeyValueStore;
import com.de314.data.local.model.Article;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.rocksdb.RocksDBException;

public class RocksDBKeyValueStoreTest extends AbstractKeyValueStoreTest {

    public static final String ARTICLE_NAMESPACE = "article";
    private static final RocksKeyValueStore ARTICLE_STORE;

    static {
        try {
            ARTICLE_STORE = RocksKeyValueStore.create("test.rocks", Article.class);
        } catch (RocksDBException e) {
            throw new RuntimeException("Could not create rocks connection", e);
        }
    }

    private static RocksKeyValueStore<Article> store;

    @BeforeAll
    public static void init() {
        store = DataStoreFactory.testInstance().getRocksStore(ARTICLE_NAMESPACE, Article.class);
    }

    @AfterAll
    public static void cleanup() {
        DataStoreFactory.testInstance().destroyRocksNamespace(ARTICLE_NAMESPACE);
    }

    @Override
    KeyValueStore<Article> getStore() {
        return store;
    }
}
