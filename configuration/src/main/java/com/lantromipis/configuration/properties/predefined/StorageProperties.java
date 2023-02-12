package com.lantromipis.configuration.properties.predefined;

import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "pg-facade.storage")
public interface StorageProperties {
    AdapterType adapter();

    FileBasedStorageProperties file();

    interface FileBasedStorageProperties {
        String directoryPath();

        String postgresNodesInfoFilename();
    }

    enum AdapterType {
        FILE
    }
}
