package com.de314.data.local;

import com.de314.data.local.api.CursorPage;
import com.de314.data.local.api.KeyValueStore;
import com.de314.data.local.api.ScanOptions;
import com.de314.data.local.model.Article;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class AbstractKeyValueStoreTest {

	private KeyValueStore<Article> store;

	@BeforeEach
	public void setup() {
		this.store = getStore();
		this.store.delete(ScanOptions.all().build());
	}

	abstract KeyValueStore<Article> getStore();

	@Test
	public void exercise() {
		assertEquals(0L, store.count());

		Article expected = Article.builder().id(1)
				.title("Hello, World!")
				.body("This is only a test!")
				.tag("test").tag("small")
				.build();
		store.put(expected.getKey(), expected);

		assertEquals(1L, store.count());

		Article actual = store.get(expected.getKey()).get();

		assertNotNull(actual);
		assertEquals(expected, actual);

		assertTrue(store.delete(expected.getKey()));
		assertFalse(store.delete(expected.getKey()));

		assertEquals(0L, store.count());
	}

	@Test
	public void exerciseScan() {
		assertEquals(0L, store.count());

		for (int i = 0; i < 50; i++) {
			Article expected = Article.builder()
					.id(i)
					.title("Hello, World!")
					.body("This is only a test!")
					.tag("test").tag("small")
					.build();
			store.put("a" + expected.getKey(), expected);
		}
		for (int i = 0; i < 50; i++) {
			Article expected = Article.builder()
					.id(i)
					.title("Hello, World!")
					.body("This is only a test!")
					.tag("test").tag("small")
					.build();
			store.put("b" + expected.getKey(), expected);
		}

		assertEquals(100L, store.count());
		assertEquals(50L, store.count(ScanOptions.fromPrefix("a").build()));
		assertEquals(50L, store.count(ScanOptions.fromPrefix("b").build()));

		assertEquals(50L, store.delete(ScanOptions.fromPrefix("a").build()));

		assertEquals(50L, store.count());
		assertEquals(0L, store.count(ScanOptions.fromPrefix("a").build()));
		assertEquals(50L, store.count(ScanOptions.fromPrefix("b").build()));

		assertEquals(50L, store.delete(ScanOptions.fromPrefix("b").build()));

		assertEquals(0L, store.count());
	}

	@Test
	public void exercisePaging() {
		assertEquals(0L, store.count());

		for (int i = 0; i < 1000; i++) {
			Article expected = Article.builder()
					.id(i)
					.title("Hello, World!")
					.body("This is only a test!")
					.tag("test").tag("small")
					.build();
			store.put("a" + expected.getKey(), expected);
		}

		int count = 0;
		Set<Long> observedIds = Sets.newHashSet();

		ScanOptions options = ScanOptions.all().limit(2L).build();
		do {
			CursorPage<Article> page = store.scan(options);
			count += page.getContent().size();
			page.getContent().forEach(row -> observedIds.add(row.getValue().getId()));
			options = page.getNext();
		} while (options != null);

		assertEquals(1000, count);
		assertEquals(1000, observedIds.size());
	}
}
