package com.lantromipis.configuration.properties.predefined;

import io.smallrye.config.ConfigMapping;

import java.time.Duration;

@ConfigMapping(prefix = "pg-facade.archiving")
public interface ArchivingProperties {

    boolean enabled();

    AdapterType adapter();

    S3ArchiverProperties s3();

    BasebackupProperties basebackup();

    WalStreamingProperties walStreaming();

    interface WalStreamingProperties {
        int uploadWalRetries();

        Duration walDirClearInterval();

        Duration retryUploadWalFilesInterval();

        WalRecoveryProperties recovery();

        interface WalRecoveryProperties {
            boolean moveWalAfterCompletion();

            Duration streamingActiveCheckInterval();

            int maxUnsuccessfulRetriesBeforeForceRestart();

            boolean createNewBackupInCaseOfForceRetry();
        }
    }

    interface BasebackupProperties {
        Duration createInterval();

        Duration listBackupsInterval();

        BasebackupCleanupProperties cleanUp();

        interface BasebackupCleanupProperties {
            boolean removeOld();

            Duration keepOldInterval();

            boolean removeOldWalFilesWhenRemoving();
        }
    }

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
