package com.de314.data.local.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NonNull;

import java.io.IOException;
import java.util.List;

public class JsonUtils {

    private static final ObjectMapper JSON_OBJECT_MAPPER = new ObjectMapper();

    private final ObjectMapper jsonObjectMapper;

    public JsonUtils() {
        this(JSON_OBJECT_MAPPER);
    }

    public JsonUtils(ObjectMapper jsonObjectMapper) {
        this.jsonObjectMapper = jsonObjectMapper;
    }

    public String asString(Object value) {
        if (value == null) {
            return "null";
        }
        try {
            return jsonObjectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return "null";
    }

    public byte[] asBytes(Object value) {
        if (value == null) {
            return new byte[0];
        }
        try {
            return jsonObjectMapper.writeValueAsBytes(value);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

    public <T> T fromJson(@NonNull String json, @NonNull Class<T> targetKind) {
        try {
            return jsonObjectMapper.readValue(json, targetKind);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public <T> T fromJson(@NonNull byte[] json, @NonNull Class<T> targetKind) {
        try {
            return jsonObjectMapper.readValue(json, targetKind);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public <T> List<T> fromJsonList(@NonNull String json, @NonNull Class<T> targetKind) {
        try {
            JavaType type = jsonObjectMapper.getTypeFactory().constructCollectionType(List.class, targetKind);
            return jsonObjectMapper.readValue(json, type);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public <T> List<T> fromJsonList(@NonNull byte[] json, @NonNull Class<T> targetKind) {
        try {
            JavaType type = jsonObjectMapper.getTypeFactory().constructCollectionType(List.class, targetKind);
            return jsonObjectMapper.readValue(json, type);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
