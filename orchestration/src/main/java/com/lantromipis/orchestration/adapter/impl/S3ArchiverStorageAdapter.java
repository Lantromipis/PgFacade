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
import com.lantromipis.orchestration.exception.AdapterInitializationException;
import com.lantromipis.orchestration.exception.UploadException;
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
    public void initialize() {
        log.info("Initializing S3 archiving adapter.");

        ArchivingProperties.S3ArchiverProperties s3ArchiverProperties = archivingProperties.s3();

        Protocol protocol =
                s3ArchiverProperties.protocol().equals(ArchivingProperties.ProtocolType.HTTP)
                        ? Protocol.HTTP
                        : Protocol.HTTPS;


        s3Client = AmazonS3ClientBuilder
                .standard()
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

        if (!walBucketExist) {
            throw new AdapterInitializationException("Bucket for WAL not found, but archiving is enabled! Create this bucket or/and change configuration!");
        }

        if (!backupsBucketExist) {
            throw new AdapterInitializationException("Bucket for backups not found, but archiving is enabled! Create this bucket or/and change configuration!");
        }

        log.info("S3 archiving adapter initialization completed!");
    }

    @Override
    public List<Instant> getBackupInstants() {
        ObjectListing backupsListing = s3Client.listObjects(
                archivingProperties.s3().backupsBucket(),
                ArchiverConstants.S3_BACKUP_PREFIX
        );

        if (CollectionUtils.isEmpty(backupsListing.getObjectSummaries())) {
            return Collections.emptyList();
        }

        return backupsListing
                .getObjectSummaries()
                .stream()
                .map(summary -> parseBackupInstantSafely(summary.getKey()))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    @Override
    public void uploadBackup(InputStream inputStream, Instant creationTime) throws UploadException {
        String key = String.format(
                ArchiverConstants.S3_BACKUP_KEY_FORMAT,
                ArchiverConstants.S3_BACKUP_KEY_DATE_TIME_FORMATTER.format(creationTime)
        );

        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(archivingProperties.s3().backupsBucket(), key);
        InitiateMultipartUploadResult initResponse = s3Client.initiateMultipartUpload(initRequest);

        Queue<PartETag> eTagQueue = new ConcurrentLinkedQueue<>();

        byte[] buf = new byte[archivingProperties.s3().multipartUploadPartSizeMb() * 1024 * 1024];
        int partsAdded = 0;

        try {
            while (true) {
                int bytesRead = IOUtils.read(inputStream, buf);

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

            inputStream.close();

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
                    ListPartsRequest listPartsRequest = new ListPartsRequest(archivingProperties.s3().backupsBucket(), key, initResponse.getUploadId());
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
        }
    }

    @Override
    public int removeBackupsAndWalOlderThanInstant(Instant instant) {
        if (instant == null) {
            return 0;
        }

        ObjectListing backupsListing = s3Client.listObjects(
                archivingProperties.s3().backupsBucket(),
                ArchiverConstants.S3_BACKUP_PREFIX
        );

        if (CollectionUtils.isEmpty(backupsListing.getObjectSummaries())) {
            return 0;
        }

        long initialAmount = backupsListing.getObjectSummaries().stream().map(summary -> parseBackupInstantSafely(summary.getKey())).filter(Objects::nonNull).count();

        // do not remove last backup
        if (initialAmount == 1) {
            return 0;
        }


        AtomicInteger removed = new AtomicInteger();

        List<S3ObjectSummary> remainingBackupsSummaries = backupsListing.getObjectSummaries()
                .stream()
                .filter(summary -> {
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
                        }
                ).toList();

        if (removed.get() != 0) {
            S3ObjectSummary oldestBackup = remainingBackupsSummaries
                    .stream()
                    .min((summary1, summary2) -> {
                        Instant backupInstant1 = parseBackupInstantSafely(summary1.getKey());
                        Instant backupInstant2 = parseBackupInstantSafely(summary2.getKey());

                        return backupInstant1.compareTo(backupInstant2);
                    })
                    .stream()
                    .findFirst()
                    .orElse(null);

            if (oldestBackup == null) {
                log.error("Removed backup but failed to remove old WAL files. Consider removing them manually.");
            } else {
                S3Object oldestBackupObject = s3Client.getObject(archivingProperties.s3().backupsBucket(), oldestBackup.getKey());
                String lastWalName = oldestBackupObject.getObjectMetadata().getUserMetaDataOf(ArchiverConstants.S3_BACKUP_FIRST_REQUIRED_WAL_METADATA_KEY);

                if (lastWalName == null) {
                    log.error("Oldest backup has corrupted {} metadata parameter. Can not remove old WAL files. Consider removing them manually.", ArchiverConstants.S3_BACKUP_FIRST_REQUIRED_WAL_METADATA_KEY);
                } else {
                    removeWalFilesOlderThan(lastWalName);
                }
            }
        }

        return removed.get();
    }

    @Override
    public void uploadWalFile(File file) throws UploadException {
        String key = String.format(
                ArchiverConstants.S3_WAL_FILE_KEY_FORMAT,
                file.getName()
        );

        PutObjectRequest putObjectRequest = new PutObjectRequest(
                archivingProperties.s3().walBucket(),
                key,
                file
        );

        try {
            s3Client.putObject(putObjectRequest);
        } catch (Exception e) {
            throw new UploadException("Failed to upload WAL file {}" + file.getName(), e);
        }
    }

    private void removeWalFilesOlderThan(String walFileName) {
        String lastRequiredWalKey = String.format(
                ArchiverConstants.S3_WAL_FILE_KEY_FORMAT,
                walFileName
        );

        ListObjectsV2Request listObjectsReqManual = new ListObjectsV2Request()
                .withBucketName(archivingProperties.s3().walBucket())
                .withPrefix(ArchiverConstants.S3_WAL_PREFIX);

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

            listObjectsReqManual = new ListObjectsV2Request()
                    .withContinuationToken(listObjectsResult.getNextContinuationToken());
        }

        List<List<DeleteObjectsRequest.KeyVersion>> partitionsBy1000Keys = ListUtils.partition(keysToRemove, 999);

        for (var partition : partitionsBy1000Keys) {
            s3Client.deleteObjects(
                    new DeleteObjectsRequest(archivingProperties.s3().walBucket())
                            .withKeys(partition)
            );
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
