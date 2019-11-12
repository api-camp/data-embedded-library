package com.de314.data.local.api.service;

public interface ArchiveStrategy {

    String getName();

    boolean offload(String namespace, String archiveFilename);

    boolean reload(String namespace);
}
