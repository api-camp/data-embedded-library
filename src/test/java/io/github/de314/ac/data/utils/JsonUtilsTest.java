package io.github.de314.ac.data.utils;

import io.github.de314.ac.data.model.Article;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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

    private JsonNode n(int id) {
        return jsonUtils.createObject().put("id", id);
    }

    @Test
    public void createObject() {
        ObjectNode actual = jsonUtils.createObject();
        assertNotNull(actual);
        assertEquals(0, actual.size());
        assertEquals("{}", actual.toString());
    }

    @Test
    public void createArray() {
        ArrayNode actual = jsonUtils.createArray();
        assertNotNull(actual);
        assertEquals(0, actual.size());
        assertEquals("[]", actual.toString());
    }

    @Test
    public void merge() {
        JsonNode a = n(1);
        JsonNode b = jsonUtils.createObject().put("num", 42);

        JsonNode expected = jsonUtils.createObject().put("id", 1).put("num", 42);

        JsonNode actual = jsonUtils.merge(a, b);

        assertEquals(expected, actual);
    }

    @Test
    public void merge_overwrite() {
        JsonNode a = n(1);
        JsonNode b = n(42);

        JsonNode expected = jsonUtils.createObject().put("id", 42);

        JsonNode actual = jsonUtils.merge(a, b);

        assertEquals(expected, actual);
    }

    @Test
    public void asString() {
        Article expected = a(1);
        String raw = jsonUtils.asString(expected);
        Article actual = jsonUtils.fromJson(raw, Article.class);

        assertEquals(expected, actual);
    }

    @Test
    public void asString_node() {
        JsonNode expected = n(1);
        String raw = jsonUtils.asString(expected);
        JsonNode actual = jsonUtils.fromJson(raw);

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
    void asBytes_node() {
        JsonNode expected = n(1);
        byte[] raw = jsonUtils.asBytes(expected);
        JsonNode actual = jsonUtils.fromJson(raw);

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