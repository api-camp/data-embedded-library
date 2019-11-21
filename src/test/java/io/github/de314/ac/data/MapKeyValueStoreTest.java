package io.github.de314.ac.data;

import io.github.de314.ac.data.api.kv.KeyValueStore;
import io.github.de314.ac.data.memory.MapKeyValueStore;
import io.github.de314.ac.data.model.Article;
import org.junit.jupiter.api.Test;

public class MapKeyValueStoreTest extends AbstractKeyValueStoreTest {

    @Override
    KeyValueStore<Article> getStore() {
        return MapKeyValueStore.create(Article.NAMESPACE);
    }

    @Test
    void test() {

    }
}
