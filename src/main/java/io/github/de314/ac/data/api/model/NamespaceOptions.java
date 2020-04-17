package io.github.de314.ac.data.api.model;

import lombok.Value;

import java.util.function.Supplier;

/**
 * Options for partitioning data.
 */
@Value
public class NamespaceOptions {

    public static final String DATA_DIRECTORY_PATH_PREFIX = "__db";
    public static final String ROCKS_DATA_DIRECTORY_PATH = "/db.rocks";
    public static final String ARCHIVE_DIRECTORY_PATH = "/archive";

    private String namespace;
    /** Root path for disk persistence. This will include storage specific files and directories, e.g. RockDB. */
    private String path;

    public String getRocksPath() {
        return path + ROCKS_DATA_DIRECTORY_PATH;
    }

    public String getArchivePath() {
        return path + ARCHIVE_DIRECTORY_PATH;
    }

    public void lock(Runnable runnable) {
        synchronized (this) {
            runnable.run();
        }
    }

    public <T> T lock(Supplier<T> supplier) {
        T value = null;
        synchronized (this) {
            value = supplier.get();
        }
        return value;
    }

    public static NamespaceOptions create(String namespace) {
        return new NamespaceOptions(namespace, DATA_DIRECTORY_PATH_PREFIX + namespace);
    }
}
