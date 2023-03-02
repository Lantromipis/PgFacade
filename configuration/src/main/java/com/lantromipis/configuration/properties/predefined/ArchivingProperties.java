package com.lantromipis.configuration.properties.predefined;

import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "pg-facade.archiving")
public interface ArchivingProperties {

    boolean enabled();

    AdapterType adapter();

    S3ArchiverProperties s3();

    interface S3ArchiverProperties {

        ProtocolType protocol();

        String endpoint();

        String accessKey();

        String secretKey();

        String region();

        String backupsBucket();

        String walBucket();

        int multipartUploadPartSizeMb();
    }

    enum AdapterType {
        S3
    }

    enum ProtocolType {
        HTTP,
        HTTPS
    }
}
