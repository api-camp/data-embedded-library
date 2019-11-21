package io.github.de314.ac.data.api.service;

import io.github.de314.ac.data.utils.FileUtils;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.function.Function;

@Slf4j
@Value
public class SpaceArchiveStrategy extends LoggingArchiveStrategy {

    private FileUtils.SpaceConfig config;

    @Override
    public boolean offload(String namespace, String archiveFilename) {
        super.offload(namespace, archiveFilename);

        String remoteName = String.format("bak.v1/%s", new File(archiveFilename).getName());
        boolean success = FileUtils.upload(config, archiveFilename, remoteName);
        if (success) {
            remoteName = getRemoteFilePath(namespace);
            success = FileUtils.upload(config, archiveFilename, remoteName);
            if (!success) {
                log.error("Failed to upload {} from {}", remoteName, archiveFilename);
            }
        } else {
            log.error("Failed to upload {} from {}", remoteName, archiveFilename);
        }

        return success;
    }

    @Override
    public boolean reload(String namespace) {
        String remoteName = String.format("bak.v1/%s-latest.zip", namespace);

        String archiveName = FileUtils.getFromSpace(config, getRemoteFilePath(namespace));

        boolean success = false;
        if (archiveName != null) {
            success = FileUtils.unzipDir(archiveName, Function.identity());
        }

        return success;
    }

    protected String getRemoteFilePath(String namespace) {
        return String.format("bak.v1/%s-latest.zip", namespace);
    }
}
