package com.lantromipis.orchestration.adapter.impl;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.devicefarm.model.ListUploadsRequest;
import com.amazonaws.services.dynamodbv2.xspec.S;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.*;
import com.lantromipis.configuration.properties.predefined.ArchivingProperties;
import com.lantromipis.orchestration.adapter.api.ArchiverAdapter;
import com.lantromipis.orchestration.adapter.api.PlatformAdapter;
import com.lantromipis.orchestration.exception.BackupCreationException;
import com.lantromipis.orchestration.model.BaseBackupAsInputStream;
import io.quarkus.arc.lookup.LookupIfProperty;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.eclipse.microprofile.context.ManagedExecutor;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Instance;
import javax.inject.Inject;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

@Slf4j
@ApplicationScoped
@LookupIfProperty(name = "pg-facade.archiving.adapter", stringValue = "s3")
public class S3ArchiverAdapter implements ArchiverAdapter {

    @Inject
    ArchivingProperties archivingProperties;

    @Inject
    Instance<PlatformAdapter> platformAdapter;

    @Inject
    ManagedExecutor managedExecutor;

    private AmazonS3 s3Client;

    private static final String METADATA_DATE = "date";

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

        s3Client.listBuckets().forEach(bucket -> log.info(bucket.getName()));

        createNewBackup();

        log.info("S3 archiving adapter initialization completed!");
    }

    @Override
    public void createNewBackup() throws BackupCreationException {
        BaseBackupAsInputStream baseBackupAsInputStream = platformAdapter.get().createBaseBackupAndGetAsStream();
        if (!baseBackupAsInputStream.isSuccess()) {
            throw new BackupCreationException("Failed to get basebackup");
        }
        try {
            Instant instant = Instant.now();

            uploadDataAsInputStream(
                    instant,
                    archivingProperties.s3().bucket(),
                    createBackupName(instant),
                    baseBackupAsInputStream.getStream()
            );
        } catch (Exception e) {
            try {
                baseBackupAsInputStream.getStream().close();
            } catch (Exception ignored) {
            }

            throw new BackupCreationException("Failed to transfer basebackup", e);
        }
    }

    private String createBackupName(Instant instant) {
        return "postgres-backup-" + instant.toString();
    }

    private void uploadDataAsInputStream(Instant instant, String bucket, String key, InputStream inputStream) throws BackupCreationException {
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.getUserMetadata().put(METADATA_DATE, instant.toString());

        InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucket, key)
                .withObjectMetadata(metadata);
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
                        .withBucketName(bucket)
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
                    bucket,
                    key,
                    initResponse.getUploadId(),
                    eTagQueue.stream().toList()
            );

            s3Client.completeMultipartUpload(completeRequest);

            inputStream.close();

        } catch (Exception e) {
            log.error("Error while uploading backup to S3. ", e);

            // free space
            while (true) {
                AbortMultipartUploadRequest abortMultipartUploadRequest = new AbortMultipartUploadRequest(
                        bucket,
                        key,
                        initResponse.getUploadId()
                );
                s3Client.abortMultipartUpload(abortMultipartUploadRequest);

                try {
                    ListPartsRequest listPartsRequest = new ListPartsRequest(bucket, key, initResponse.getUploadId());
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
        }
    }


}
