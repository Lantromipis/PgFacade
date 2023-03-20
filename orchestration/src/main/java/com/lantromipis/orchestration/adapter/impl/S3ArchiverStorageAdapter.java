package com.lantromipis.orchestration.adapter.impl;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.lantromipis.configuration.properties.predefined.ArchivingProperties;
import com.lantromipis.orchestration.adapter.api.ArchiverStorageAdapter;
import com.lantromipis.orchestration.constant.ArchiverConstants;
import com.lantromipis.orchestration.exception.InitializationException;
import com.lantromipis.orchestration.exception.DownloadException;
import com.lantromipis.orchestration.exception.UploadException;
import com.lantromipis.orchestration.model.BaseBackupDownload;
import com.lantromipis.orchestration.model.WalFileDownload;
import io.quarkus.arc.lookup.LookupIfProperty;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.io.IOUtils;
import org.eclipse.microprofile.context.ManagedExecutor;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

@Slf4j
@ApplicationScoped
@LookupIfProperty(name = "pg-facade.archiving.adapter", stringValue = "s3")
public class S3ArchiverStorageAdapter implements ArchiverStorageAdapter {

    @Inject
    ArchivingProperties archivingProperties;

    @Inject
    ManagedExecutor managedExecutor;

    private AmazonS3 s3Client;

    @Override
    public void initializeAndValidate() throws InitializationException {
        log.info("Initializing S3 archiving adapter.");

        ArchivingProperties.S3ArchiverProperties s3ArchiverProperties = archivingProperties.s3();

        Protocol protocol = s3ArchiverProperties.protocol().equals(ArchivingProperties.ProtocolType.HTTP) ? Protocol.HTTP : Protocol.HTTPS;


        s3Client = AmazonS3ClientBuilder.standard()
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(
                                s3ArchiverProperties.endpoint(),
                                Regions.US_EAST_1.name()
                        )
                )
                .withPathStyleAccessEnabled(true)
                .withClientConfiguration(
                        new ClientConfiguration()
                                .withProtocol(protocol)
                                .withConnectionTimeout(30000)
                )
                .withCredentials(
                        new AWSStaticCredentialsProvider(
                                new BasicAWSCredentials(
                                        s3ArchiverProperties.accessKey(),
                                        s3ArchiverProperties.secretKey()
                                )
                        )
                )
                .build();

        boolean walBucketExist = false;
        boolean backupsBucketExist = false;

        try {
            // connectivity check + validation of buckets presence
            for (var bucket : s3Client.listBuckets()) {
                if (bucket.getName().equals(archivingProperties.s3().backupsBucket())) {
                    log.info("Found bucket for backups in S3!");
                    backupsBucketExist = true;
                }
                if (bucket.getName().equals(archivingProperties.s3().walBucket())) {
                    log.info("Found bucket for WAL files in S3!");
                    walBucketExist = true;
                }
            }
        } catch (Exception e) {
            throw new InitializationException("Failed to check buckets in S3! Have you configured s3 properly?");
        }

        if (!walBucketExist) {
            throw new InitializationException("Bucket for WAL not found, but archiving is enabled! Create this bucket or/and change configuration!");
        }

        if (!backupsBucketExist) {
            throw new InitializationException("Bucket for backups not found, but archiving is enabled! Create this bucket or/and change configuration!");
        }

        log.info("S3 archiving adapter initialization completed!");
    }

    @Override
    public List<Instant> getBackupInstants() {
        try {
            ObjectListing backupsListing = s3Client.listObjects(archivingProperties.s3().backupsBucket(), ArchiverConstants.S3_BACKUP_PREFIX);

            if (CollectionUtils.isEmpty(backupsListing.getObjectSummaries())) {
                return Collections.emptyList();
            }

            return backupsListing.getObjectSummaries().stream().map(summary -> parseBackupInstantSafely(summary.getKey())).filter(Objects::nonNull).collect(Collectors.toList());
        } catch (Exception e) {
            log.error("Failed to retrieve backups list.", e);
            return Collections.emptyList();
        }
    }

    @Override
    public void uploadBackupTar(InputStream inputStreamWithBackupTar, Instant creationTime, String firstWalFileName) throws UploadException {
        String key = String.format(ArchiverConstants.S3_BACKUP_KEY_FORMAT, ArchiverConstants.S3_BACKUP_KEY_DATE_TIME_FORMATTER.format(creationTime));

        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(archivingProperties.s3().backupsBucket(), key);
        if (firstWalFileName == null || firstWalFileName.length() != ArchiverConstants.WAL_FILE_NAME_LENGTH) {
            log.error("Invalid first WAL file name. Backup will be created, but auto-remove WAL files wont work.");
        } else {
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.addUserMetadata(ArchiverConstants.S3_BACKUP_FIRST_REQUIRED_WAL_METADATA_KEY, firstWalFileName);
            initRequest.setObjectMetadata(metadata);
        }

        InitiateMultipartUploadResult initResponse;

        try {
            initResponse = s3Client.initiateMultipartUpload(initRequest);
        } catch (Exception e) {
            try {
                inputStreamWithBackupTar.close();
            } catch (Exception ignored) {
            }
            throw new UploadException("Failed to start backup upload ", e);
        }

        Queue<PartETag> eTagQueue = new ConcurrentLinkedQueue<>();

        byte[] buf = new byte[archivingProperties.s3().multipartUploadPartSizeMb() * 1024 * 1024];
        int partsAdded = 0;

        try {
            while (true) {
                int bytesRead = IOUtils.read(inputStreamWithBackupTar, buf);

                if (bytesRead == 0) {
                    break;
                }

                UploadPartRequest uploadRequest = new UploadPartRequest()
                        .withBucketName(archivingProperties.s3().backupsBucket())
                        .withKey(key)
                        .withUploadId(initResponse.getUploadId())
                        .withPartSize(bytesRead)
                        .withPartNumber(partsAdded)
                        .withInputStream(new ByteArrayInputStream(buf));


                UploadPartResult uploadResult = s3Client.uploadPart(uploadRequest);
                eTagQueue.add(uploadResult.getPartETag());

                partsAdded++;
            }

            CompleteMultipartUploadRequest completeRequest = new CompleteMultipartUploadRequest(
                    archivingProperties.s3().backupsBucket(),
                    key,
                    initResponse.getUploadId(),
                    eTagQueue.stream().toList()
            );

            s3Client.completeMultipartUpload(completeRequest);

        } catch (Exception e) {

            // free space
            while (true) {
                AbortMultipartUploadRequest abortMultipartUploadRequest = new AbortMultipartUploadRequest(
                        archivingProperties.s3().backupsBucket(),
                        key,
                        initResponse.getUploadId()
                );
                s3Client.abortMultipartUpload(abortMultipartUploadRequest);

                try {
                    ListPartsRequest listPartsRequest = new ListPartsRequest(
                            archivingProperties.s3().backupsBucket(),
                            key,
                            initResponse.getUploadId()
                    );
                    PartListing partListing = s3Client.listParts(listPartsRequest);

                    if (CollectionUtils.isEmpty(partListing.getParts())) {
                        break;
                    }

                    Thread.sleep(1000);
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                } catch (Exception ignored) {
                    break;
                }
            }

            throw new UploadException("Failed to upload backup! ", e);
        } finally {
            try {
                inputStreamWithBackupTar.close();
            } catch (Exception ignored) {
            }
        }
    }

    @Override
    public int removeBackupsAndWalOlderThanInstant(Instant instant, boolean removeWal) {
        try {
            if (instant == null) {
                return 0;
            }

            ObjectListing backupsListing = s3Client.listObjects(archivingProperties.s3().backupsBucket(), ArchiverConstants.S3_BACKUP_PREFIX);

            if (CollectionUtils.isEmpty(backupsListing.getObjectSummaries())) {
                return 0;
            }

            long initialAmount = backupsListing.getObjectSummaries().stream().map(summary -> parseBackupInstantSafely(summary.getKey())).filter(Objects::nonNull).count();

            // do not remove last backup
            if (initialAmount == 1) {
                return 0;
            }


            AtomicInteger removed = new AtomicInteger();

            List<S3ObjectSummary> remainingBackupsSummaries = backupsListing.getObjectSummaries().stream().filter(summary -> {
                Instant backupInstant = parseBackupInstantSafely(summary.getKey());

                if (backupInstant == null) {
                    return false;
                }

                // do not remove last backup
                if (initialAmount - removed.get() <= 1) {
                    return true;
                }

                if (backupInstant.compareTo(instant) < 0) {
                    log.info("Removing backup with key '{}' as it is too old.", summary.getKey());
                    s3Client.deleteObject(archivingProperties.s3().backupsBucket(), summary.getKey());
                    removed.getAndIncrement();
                    return false;
                } else {
                    return true;
                }
            }).toList();

            if (removed.get() != 0) {
                S3ObjectSummary oldestBackupObjectSummary = remainingBackupsSummaries.stream().min((summary1, summary2) -> {
                    Instant backupInstant1 = parseBackupInstantSafely(summary1.getKey());
                    Instant backupInstant2 = parseBackupInstantSafely(summary2.getKey());

                    return backupInstant1.compareTo(backupInstant2);
                }).stream().findFirst().orElse(null);

                if (oldestBackupObjectSummary == null) {
                    log.error("Removed backup but failed to remove old WAL files. Consider removing them manually.");
                } else {
                    ObjectMetadata metadata = s3Client.getObjectMetadata(archivingProperties.s3().backupsBucket(), oldestBackupObjectSummary.getKey());
                    String lastWalName = metadata.getUserMetaDataOf(ArchiverConstants.S3_BACKUP_FIRST_REQUIRED_WAL_METADATA_KEY);

                    if (removeWal) {
                        if (lastWalName == null || lastWalName.length() != ArchiverConstants.WAL_FILE_NAME_LENGTH) {
                            log.error("Oldest backup has corrupted {} metadata parameter. Can not remove old WAL files. Consider removing them manually.", ArchiverConstants.S3_BACKUP_FIRST_REQUIRED_WAL_METADATA_KEY);
                        } else {
                            removeWalFilesOlderThan(lastWalName);
                        }
                    }
                }
            }

            return removed.get();
        } catch (Exception e) {
            log.error("Failed to remove old backups. ", e);
            return 0;
        }
    }

    @Override
    public void uploadWalFile(File file, Instant createdWhen) throws UploadException {
        try {
            String key = String.format(ArchiverConstants.S3_WAL_FILE_KEY_FORMAT, file.getName());

            ObjectMetadata objectMetadata = new ObjectMetadata();
            objectMetadata.addUserMetadata(ArchiverConstants.S3_WAL_INSTANT_METADATA_KEY, Instant.now().toString());

            PutObjectRequest putObjectRequest = new PutObjectRequest(archivingProperties.s3().walBucket(), key, file);

            putObjectRequest.setMetadata(objectMetadata);

            s3Client.putObject(putObjectRequest);
        } catch (Exception e) {
            throw new UploadException("Failed to upload WAL file " + file.getName(), e);
        }
    }

    @Override
    public WalFileDownload downloadWalFile(String walFileName) throws DownloadException {
        try {
            String walFileKey = String.format(ArchiverConstants.S3_WAL_FILE_KEY_FORMAT, walFileName);

            S3Object walObject = s3Client.getObject(archivingProperties.s3().walBucket(), walFileKey);

            return WalFileDownload.builder().inputStream(walObject.getObjectContent()).build();

        } catch (Exception e) {
            throw new DownloadException("Failed to download WAL file " + walFileName + " ", e);
        }
    }

    @Override
    public BaseBackupDownload downloadBaseBackup(Instant instant) throws DownloadException {
        try {
            ObjectListing backupsListing = s3Client.listObjects(archivingProperties.s3().backupsBucket(), ArchiverConstants.S3_BACKUP_PREFIX);

            S3ObjectSummary targetBackupSummary = backupsListing.getObjectSummaries().stream().filter(summary -> {
                Instant backupInstant = parseBackupInstantSafely(summary.getKey());

                if (backupInstant != null && backupInstant.equals(instant)) {
                    return true;
                }

                return false;
            }).findFirst().orElse(null);

            if (targetBackupSummary == null) {
                throw new DownloadException("Backup for provided instant " + ArchiverConstants.S3_BACKUP_KEY_DATE_TIME_FORMATTER.format(instant) + " not found. ");
            }

            S3Object backup = s3Client.getObject(archivingProperties.s3().backupsBucket(), targetBackupSummary.getKey());

            String firstWalFile = backup.getObjectMetadata().getUserMetaDataOf(ArchiverConstants.S3_BACKUP_FIRST_REQUIRED_WAL_METADATA_KEY);

            if (firstWalFile == null || firstWalFile.length() < ArchiverConstants.WAL_FILE_NAME_LENGTH) {
                throw new DownloadException("Corrupted metadata for backup. No metadata '" + ArchiverConstants.S3_BACKUP_FIRST_REQUIRED_WAL_METADATA_KEY + "' provided.");
            }

            return BaseBackupDownload.builder().firstWalFile(firstWalFile).inputStreamWithBackupTar(backup.getObjectContent()).build();

        } catch (DownloadException e) {
            throw e;
        } catch (Exception e) {
            throw new DownloadException("Failed to retrieve backup", e);
        }
    }

    @Override
    public List<String> getAllWalFileNamesSortedStartingFrom(String firstWalFileName) {
        try {
            return getAllWalFilesKeys()
                    .stream()
                    .map(key -> key.replace(ArchiverConstants.S3_WAL_PREFIX_WITH_SLASH, ""))
                    .filter(name -> name.compareTo(firstWalFileName) >= 0)
                    .sorted()
                    .collect(Collectors.toList());
        } catch (Exception e) {
            log.error("Failed to retrieve list of wal files starting from {}", firstWalFileName, e);
            return Collections.emptyList();
        }
    }

    private List<String> getAllWalFilesKeys() {
        List<String> allWalFiles = new ArrayList<>();

        ListObjectsV2Request listObjectsReqManual = new ListObjectsV2Request()
                .withBucketName(archivingProperties.s3().walBucket())
                .withPrefix(ArchiverConstants.S3_WAL_PREFIX);

        boolean done = false;
        while (!done) {
            ListObjectsV2Result listObjectsResult = s3Client.listObjectsV2(listObjectsReqManual);

            allWalFiles.addAll(listObjectsResult.getObjectSummaries().stream().map(S3ObjectSummary::getKey).toList());

            if (listObjectsResult.getNextContinuationToken() == null) {
                done = true;
            }

            listObjectsReqManual = new ListObjectsV2Request().withContinuationToken(listObjectsResult.getNextContinuationToken());
        }

        return allWalFiles;
    }

    private void removeWalFilesOlderThan(String walFileName) {
        // check wal file name format before calling!!!
        String lastRequiredWalKey = String.format(ArchiverConstants.S3_WAL_FILE_KEY_FORMAT, walFileName);

        ListObjectsV2Request listObjectsReqManual = new ListObjectsV2Request().withBucketName(archivingProperties.s3().walBucket()).withPrefix(ArchiverConstants.S3_WAL_PREFIX);

        List<DeleteObjectsRequest.KeyVersion> keysToRemove = new ArrayList<>();

        boolean done = false;
        while (!done) {
            ListObjectsV2Result listObjectsResult = s3Client.listObjectsV2(listObjectsReqManual);

            for (S3ObjectSummary summary : listObjectsResult.getObjectSummaries()) {
                if (summary.getKey().compareTo(lastRequiredWalKey) < 0) {
                    keysToRemove.add(new DeleteObjectsRequest.KeyVersion(summary.getKey()));
                }
            }

            if (listObjectsResult.getNextContinuationToken() == null) {
                done = true;
            }

            listObjectsReqManual = new ListObjectsV2Request().withContinuationToken(listObjectsResult.getNextContinuationToken());
        }

        List<List<DeleteObjectsRequest.KeyVersion>> partitionsBy1000Keys = ListUtils.partition(keysToRemove, 999);

        for (var partition : partitionsBy1000Keys) {
            s3Client.deleteObjects(new DeleteObjectsRequest(archivingProperties.s3().walBucket()).withKeys(partition));
        }

        log.info("Removed {} old wal files which are not required.", keysToRemove.size());
    }

    private Instant parseBackupInstantSafely(String backupKey) {
        try {
            Matcher matcher = ArchiverConstants.S3_BACKUP_KEY_PATTERN.matcher(backupKey);

            if (!matcher.matches()) {
                log.error("Unable to check backup creation time for object with key '{}'. Did you change tha name of the object? You will need to remove it manually now.", backupKey);
                return null;
            }

            return ArchiverConstants.S3_BACKUP_KEY_DATE_TIME_FORMATTER.parse(matcher.group(1), Instant::from);
        } catch (Exception e) {
            log.error("Unable to check backup creation time for object with key '{}'. Did you change tha name of the object? You will need to remove it manually now.", backupKey);
            return null;
        }
    }
}
