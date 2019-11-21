package io.github.de314.ac.data.api.service;

import io.github.de314.ac.data.utils.FileUtils;
import com.google.common.collect.Maps;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.Map;
import java.util.function.Function;

/**
 * TODO: this will not work across application restarts.
 * Need to implement the following:
 *  - offload:
 *      1. Move file to tmp archive directory for disk backups (configurable)
 *      2. Ensure that file has some sense of version or time stamp in filename
 *  - reload
 *      1. Search disk for files matching namespace
 *      2. Pick latest version if multiple present
 *      3. Extract previous state
 */
@Slf4j
@Value
public class DiskArchiveStrategy extends LoggingArchiveStrategy {

    private final Map<String, String> archiveCache = Maps.newHashMap();

    @Override
    public boolean offload(String namespace, String archiveFilename) {
        super.offload(namespace, archiveFilename);

        archiveCache.put(namespace, archiveFilename);

        return true;
    }

    @Override
    public boolean reload(String namespace) {
        String archiveName = archiveCache.get(namespace);

        boolean success = archiveName != null;

        if (success) {
            success = FileUtils.unzipDir(archiveName, Function.identity());
        }

        return success;
    }

    public void cleanup() {
        for (String archiveName : archiveCache.values()) {
            try {
                File archive = new File(archiveName);
                if (archive.exists()) {
                    archive.delete();
                }
            } catch (Throwable t) {
                log.error("Failed to cleanup {}", archiveName);
            }
        }
    }
}
