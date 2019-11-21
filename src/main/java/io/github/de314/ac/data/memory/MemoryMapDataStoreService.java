package io.github.de314.ac.data.memory;

import io.github.de314.ac.data.api.kv.KeyValueStore;
import io.github.de314.ac.data.api.service.AbstractDataStoreService;
import io.github.de314.ac.data.api.service.DataStoreService;
import com.fasterxml.jackson.databind.JsonNode;


public class MemoryMapDataStoreService extends AbstractDataStoreService {

    public static final MemoryMapDataStoreService INSTANCE = new MemoryMapDataStoreService();

    public MemoryMapDataStoreService() {
        super(MapKeyValueStore.STORE_KIND);
    }

    @Override
    protected KeyValueStore<JsonNode> create(String namespace) {
        return MapKeyValueStore.create(namespace);
    }

    @Override
    protected <ValueT> KeyValueStore<ValueT> create(String namespace, Class<ValueT> modelClass) {
        return MapKeyValueStore.create(namespace);
    }

    public static DataStoreService instance() {
        return INSTANCE;
    }
}
