package com.de314.data.local.memory;

import com.de314.data.local.api.kv.KeyValueStore;
import com.de314.data.local.api.service.AbstractDataStoreService;
import com.de314.data.local.api.service.DataStoreService;
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
