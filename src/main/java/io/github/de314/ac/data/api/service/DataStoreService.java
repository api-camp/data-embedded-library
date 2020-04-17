package io.github.de314.ac.data.api.service;

import io.github.de314.ac.data.api.kv.KeyValueStore;
import io.github.de314.ac.data.api.model.KVInfo;
import com.fasterxml.jackson.databind.JsonNode;
import io.github.de314.ac.data.api.service.archive.ArchiveStrategy;

import java.util.List;
import java.util.Optional;

public interface DataStoreService {

    String getStoreKind();

    List<KVInfo> entries();
    Optional<KVInfo> getInfo(String namespace);

    Optional<KeyValueStore<JsonNode>> get(String namespace);
    KeyValueStore<JsonNode> getOrCreate(String namespace);

    <T> Optional<KeyValueStore<T>> get(String namespace, Class<T> modelClass);
    <T> KeyValueStore<T> getOrCreate(String namespace, Class<T> modelClass);

    void backup(String namespace, ArchiveStrategy archiveStrategy);

    void rollback(String namespace, ArchiveStrategy archiveStrategy);

    void destroy(String namespace);
}
