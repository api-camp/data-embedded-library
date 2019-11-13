package com.de314.data.local;

import com.de314.data.local.api.kv.KeyValueStore;
import com.de314.data.local.api.service.DataStoreService;
import com.de314.data.local.api.service.DiskArchiveStrategy;
import com.de314.data.local.api.service.LoggingArchiveStrategy;
import com.de314.data.local.disk.RockDBDataStoreService;
import com.de314.data.local.model.Article;
import com.de314.data.local.utils.FileUtils;
import com.google.common.collect.Lists;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Slf4j
public class RocksDBKeyValueStoreTest extends AbstractKeyValueStoreTest {

    private static final DataStoreService STORE_SERVICE = RockDBDataStoreService.instance();
    private static final List<String> NAMESPACES = Lists.newArrayList();
    private static final DiskArchiveStrategy ARCHIVE_STRATEGY = new DiskArchiveStrategy();

    public static final String NAMESPACE_ARTICLE = createNamespace(Article.NAMESPACE);
    public static final String NAMESPACE_ART_2 = createNamespace("__test_art2");
    public static final String NAMESPACE_ART_3 = createNamespace("__test_art3");

    private static String createNamespace(String namespace) {
        NAMESPACES.add(namespace);
        return namespace;
    }

    private static KeyValueStore<Article> store;

    @BeforeAll
    public static void init() {
        store = STORE_SERVICE.getOrCreate(NAMESPACE_ARTICLE, Article.class);
    }

    @AfterAll
    public static void cleanup() {
        ARCHIVE_STRATEGY.cleanup();
        for (String namespace : NAMESPACES) {
            try {
                STORE_SERVICE.destroy(namespace);
            } catch (Throwable t) {
                log.error("Failed to cleanup {}", namespace, t);
            }
        }
    }

    @Override
    KeyValueStore<Article> getStore() {
        return store;
    }

    @Test
    public void backup() {
        STORE_SERVICE.backup(NAMESPACE_ARTICLE, TestArchiveStrategy.of());
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

    @Test
    public void recover() {
        Article a = Article.builder().id(1).build();
        Article b = Article.builder().id(2).build();
        String namespace = NAMESPACE_ART_3;

        KeyValueStore<Article> store = STORE_SERVICE.getOrCreate(namespace, Article.class);
        assertNotNull(store);
        long baseCount = store.count();
        if (baseCount > 0) {
            log.warn("baseCount={} is greater than 0. Namespace is not empty", baseCount);
        }

        Consumer<Article> writer = art -> store.put(art.getKey(), art);
        BiConsumer<Article, Long> tester = (art, count) -> {
            assertEquals(count, store.count());
            assertEquals(art, store.get(art.getKey()).orElse(null));
        };

        writer.accept(a);
        tester.accept(a, baseCount + 1);

        STORE_SERVICE.backup(namespace, ARCHIVE_STRATEGY);

        writer.accept(b);
        tester.accept(b, baseCount + 2);

        STORE_SERVICE.rollback(namespace, ARCHIVE_STRATEGY);

        tester.accept(a, baseCount + 1);
        assertFalse(store.get(b.getKey()).isPresent());
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
                    FileUtils.unzipDir(archiveFilename, null);
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
