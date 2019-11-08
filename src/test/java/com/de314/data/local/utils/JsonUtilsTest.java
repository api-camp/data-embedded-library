package com.de314.data.local.utils;

import com.de314.data.local.model.Article;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class JsonUtilsTest {

    private final JsonUtils jsonUtils = new JsonUtils();

    private Article a(long id) {
        return Article.builder()
                .id(id)
                .title("title")
                .body("body")
                .tag("tag1").tag("tag2")
                .build();
    }

    @Test
    public void asString() {
        Article expected = a(1);
        String raw = jsonUtils.asString(expected);
        Article actual = jsonUtils.fromJson(raw, Article.class);

        assertEquals(expected, actual);
    }

    @Test
    void asBytes() {
        Article expected = a(1);
        byte[] raw = jsonUtils.asBytes(expected);
        Article actual = jsonUtils.fromJson(raw, Article.class);

        assertEquals(expected, actual);
    }

    @Test
    void fromJsonListString() {
        List<Article> expected = Lists.newArrayList(a(1), a(2));
        String raw = jsonUtils.asString(expected);
        List<Article> actual = jsonUtils.fromJsonList(raw, Article.class);

        assertEquals(expected, actual);
    }

    @Test
    void fromJsonListBytes() {
        List<Article> expected = Lists.newArrayList(a(1), a(2));
        byte[] raw = jsonUtils.asBytes(expected);
        List<Article> actual = jsonUtils.fromJsonList(raw, Article.class);

        assertEquals(expected, actual);
    }
}