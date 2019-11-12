package com.de314.data.local;

import com.de314.data.local.api.kv.KeyValueStore;
import com.de314.data.local.api.service.ArchiveStrategy;
import com.de314.data.local.api.service.DataStoreService;
import com.de314.data.local.api.service.LoggingArchiveStrategy;
import com.de314.data.local.disk.RockDBDataStoreService;
import com.de314.data.local.disk.RocksKeyValueStore;
import com.de314.data.local.model.Article;
import com.de314.data.local.utils.FileUtils;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class RocksDBKeyValueStoreTest extends AbstractKeyValueStoreTest {

    public static final DataStoreService STORE_SERVICE = RockDBDataStoreService.instance();
    public static final String NAMESPACE_ART_2 = "art2";
    private static KeyValueStore<Article> store;

    @BeforeAll
    public static void init() {
        store = STORE_SERVICE.getOrCreate(Article.NAMESPACE, Article.class);
    }

    @AfterAll
    public static void cleanup() {
        STORE_SERVICE.destroy(Article.NAMESPACE);
        STORE_SERVICE.destroy(NAMESPACE_ART_2);
    }

    @Override
    KeyValueStore<Article> getStore() {
        return store;
    }

    @Test
    public void backup() {
        STORE_SERVICE.backup(Article.NAMESPACE, TestArchiveStrategy.of());
    }

    @Test
    public void multiConnect() {
        KeyValueStore<Article> store = STORE_SERVICE.getOrCreate(NAMESPACE_ART_2, Article.class);

        assertNotNull(store);

        Article a = Article.builder().id(1).build();

        assertEquals(0L, store.count());
        store.put(a.getKey(), a);
        assertEquals(1L, store.count());
        assertNotNull(store.get(a.getKey()));

        STORE_SERVICE.destroy(NAMESPACE_ART_2);

        store = STORE_SERVICE.getOrCreate(NAMESPACE_ART_2, Article.class);
        assertNotNull(store);

        assertEquals(0L, store.count());
        store.put(a.getKey(), a);
        assertEquals(1L, store.count());
        assertNotNull(store.get(a.getKey()));
    }

    @Data(staticConstructor = "of")
    private static class TestArchiveStrategy extends LoggingArchiveStrategy {

        @Override
        public boolean offload(String namespace, String archiveFilename) {
            super.offload(namespace, archiveFilename);
            assertNotNull(archiveFilename);
            try {
                File archiveFile = new File(archiveFilename);
                if (archiveFile.exists()) {
                    FileUtils.unzipDir(archiveFilename);
                    archiveFile.delete();
                } else {
                    fail("Archive file was not created: " + archiveFilename);
                }
            } catch (Throwable t) {
                t.printStackTrace();
                fail("Caught error: " + t.getMessage());
            }
            return true;
        }
    }
}
