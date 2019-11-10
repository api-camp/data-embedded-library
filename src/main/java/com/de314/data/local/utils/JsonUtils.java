package com.de314.data.local.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.NonNull;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Iterator;
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

    public ObjectNode createObject() {
        return jsonObjectMapper.createObjectNode();
    }

    public ArrayNode createArray() {
        return jsonObjectMapper.createArrayNode();
    }

    public JsonNode merge(@NonNull JsonNode a, @NonNull JsonNode b) {
        JsonNode result = a.isObject() ? jsonObjectMapper.createObjectNode() : jsonObjectMapper.createArrayNode();

        result = applyMerge(b, result);
        result = applyMerge(a, result);

        return result;
    }

    /*
     * Thanks! https://stackoverflow.com/a/32447591
     */
    public JsonNode applyMerge(JsonNode mainNode, JsonNode updateNode) {

        Iterator<String> fieldNames = updateNode.fieldNames();

        while (fieldNames.hasNext()) {
            String updatedFieldName = fieldNames.next();
            JsonNode valueToBeUpdated = mainNode.get(updatedFieldName);
            JsonNode updatedValue = updateNode.get(updatedFieldName);

            // If the node is an @ArrayNode
            if (valueToBeUpdated != null && valueToBeUpdated.isArray() &&
                    updatedValue.isArray()) {
                // running a loop for all elements of the updated ArrayNode
                for (int i = 0; i < updatedValue.size(); i++) {
                    JsonNode updatedChildNode = updatedValue.get(i);
                    // Create a new Node in the node that should be updated, if there was no corresponding node in it
                    // Use-case - where the updateNode will have a new element in its Array
                    if (valueToBeUpdated.size() <= i) {
                        ((ArrayNode) valueToBeUpdated).add(updatedChildNode);
                    }
                    // getting reference for the node to be updated
                    JsonNode childNodeToBeUpdated = valueToBeUpdated.get(i);
                    merge(childNodeToBeUpdated, updatedChildNode);
                }
                // if the Node is an @ObjectNode
            } else if (valueToBeUpdated != null && valueToBeUpdated.isObject()) {
                merge(valueToBeUpdated, updatedValue);
            } else {
                if (mainNode instanceof ObjectNode) {
                    ((ObjectNode) mainNode).replace(updatedFieldName, updatedValue);
                }
            }
        }
        return mainNode;
    }

    public String asString(JsonNode node) {
        if (node == null) {
            return "null";
        }
        return node.toString();
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

    public byte[] asBytes(JsonNode node) {
        if (node == null) {
            return new byte[0];
        }
        return node.toString().getBytes(Charset.defaultCharset());
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

    public JsonNode fromJson(@NonNull String json) {
        try {
            return jsonObjectMapper.readTree(json);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public JsonNode fromJson(@NonNull byte[] json) {
        try {
            return jsonObjectMapper.readTree(json);
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
