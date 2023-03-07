package com.lantromipis.orchestration.adapter.api;

import com.lantromipis.orchestration.exception.UploadException;

import java.io.File;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;

/**
 * Interface for any archiver storage adapter. Different implementations allow PgFacade to use different systems to store backups and WAL files.
 * For example, S3, FTP etc.
 * <p>
 * For easiness, PgFacade references backups by their creation Instance.
 */
public interface ArchiverStorageAdapter {
    void initialize();

    /**
     * Searches for all available backups and their creation time
     *
     * @return a list of Instant objects, each representing backup creation time. If no backups available, returns empty list.
     */
    List<Instant> getBackupInstants();

    /**
     * Uploads backup to storage
     *
     * @param inputStream  stream containing backup files. Uploaded as-is. Will be closed after completion.
     * @param creationTime will be used to name file containing backup.
     * @throws UploadException if something went wrong and upload failed.
     */
    void uploadBackup(InputStream inputStream, Instant creationTime, String firstWalFileName) throws UploadException;

    /**
     * Removes backups which are older than provided timestamp
     *
     * @param instant timestamp to compare to
     * @return number of removed backups
     */
    int removeBackupsAndWalOlderThanInstant(Instant instant, boolean removeWal);

    /**
     * Uploads WAL file to storage.
     *
     * @param file WAL file to upload. Will NOT be closed after completion.
     * @throws UploadException if something went wrong and upload failed.
     */
    void uploadWalFile(File file) throws UploadException;
}
