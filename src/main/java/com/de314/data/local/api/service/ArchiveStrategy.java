package com.de314.data.local.api.service;

public interface ArchiveStrategy {

    default String getName() {
        return getClass().getSimpleName();
    }

    boolean offload(String namespace, String archiveFilename);

    boolean reload(String namespace);
}
