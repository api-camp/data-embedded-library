package io.github.de314.ac.data.api.service;

public interface ArchiveStrategy {

    default String getName() {
        return getClass().getSimpleName();
    }

    boolean offload(String namespace, String archiveFilename);

    boolean reload(String namespace);
}
