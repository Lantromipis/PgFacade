package com.lantromipis.orchestration.adapter.api;

import com.lantromipis.orchestration.exception.DownloadException;
import com.lantromipis.orchestration.exception.InitializationException;
import com.lantromipis.orchestration.exception.UploadException;
import com.lantromipis.orchestration.model.BaseBackupDownload;
import com.lantromipis.orchestration.model.WalFileDownload;

import java.io.File;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;

/**
 * Interface for any archiver storage adapter. Different implementations allow PgFacade to use different systems to store backups and WAL files.
 * For example, S3, FTP etc.
 * <p>
 * For easiness, PgFacade references backups by their creation Instance and first required WAL.
 */
public interface ArchiverStorageAdapter {

    /**
     * Initializes adapter and performs basic calls to check if initialized successfully and if configuration is valid.
     *
     * @throws InitializationException if failed to initialize
     */
    void initializeAndValidateStorageAvailability() throws InitializationException;

    /**
     * Searches for all available backups and their creation time
     *
     * @return a list of Instant objects, each representing backup creation time. If no backups available, returns empty list.
     */
    List<Instant> getBackupInstants();

    /**
     * Uploads backup to storage as .tar
     *
     * @param inputStreamWithBackupTar stream containing backup. Backup must be in .tar format. Uploaded as-is. Will be closed after completion.
     * @param creationTime             will be used to name file containing backup.
     * @throws UploadException if something went wrong and upload failed.
     */
    void uploadBackupTar(InputStream inputStreamWithBackupTar, Instant creationTime, String firstWalFileName) throws UploadException;

    /**
     * Removes backups which are older than provided timestamp. However, must never keep at least one backup.
     * For example, if only one but very old backup exists, this method must not remove it.
     *
     * @param instant timestamp to compare to
     * @return number of removed backups
     */
    int removeBackupsAndWalOlderThanInstant(Instant instant, boolean removeWal);

    /**
     * Uploads WAL file to storage.
     *
     * @param file WAL file to upload. Will NOT be closed after completion.
     * @throws UploadException if something went wrong and upload failed
     */
    void uploadWalFile(File file) throws UploadException;

    /**
     * Downloads WAL file by its name
     *
     * @param walFileName name of WAL file
     * @return object containing inputStream with WAL file
     * @throws DownloadException if something went wrong and download failed
     */
    WalFileDownload downloadWalFile(String walFileName) throws DownloadException;

    /**
     * Downloads backup by creation timestamp
     *
     * @param instant timestamp for backup
     * @return object containing inputStream with backup and first wal file required by this backup
     * @throws DownloadException if something went wrong and download failed
     */
    BaseBackupDownload downloadBaseBackup(Instant instant) throws DownloadException;

    /**
     * Returns all available WAL files starting from provided WAL file name. If some error happens or there are no WAL files, returns empty list.
     *
     * @param firstWalFileName first WAL file name to return
     * @return sorted list containing WAL file names where first WAL file name is the oldest. Can also return empty list, if no WAL found.
     */
    List<String> getAllWalFileNamesSortedStartingFrom(String firstWalFileName);
}
