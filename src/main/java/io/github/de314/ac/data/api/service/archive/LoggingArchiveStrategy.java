package io.github.de314.ac.data.api.service.archive;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LoggingArchiveStrategy implements ArchiveStrategy {

    @Override
    public boolean offload(String namespace, String archiveFilename) {
        log.info("OFFLOAD: namespace={} archiveFilename={}", namespace, archiveFilename);
        return true;
    }

    @Override
    public boolean reload(String namespace) {
        log.info("RELOAD: namespace={}", namespace);
        return true;
    }
}
