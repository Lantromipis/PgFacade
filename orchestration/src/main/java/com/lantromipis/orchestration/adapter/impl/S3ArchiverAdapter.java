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
import com.lantromipis.orchestration.adapter.api.ArchiverAdapter;
import com.lantromipis.orchestration.constant.ArchiverConstants;
import com.lantromipis.orchestration.exception.UploadException;
import io.quarkus.arc.lookup.LookupIfProperty;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.eclipse.microprofile.context.ManagedExecutor;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

@Slf4j
@ApplicationScoped
@LookupIfProperty(name = "pg-facade.archiving.adapter", stringValue = "s3")
public class S3ArchiverAdapter implements ArchiverAdapter {

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

        s3Client.listBuckets().forEach(bucket -> log.info(bucket.getName()));

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
                .map(summary -> {
                    Matcher matcher = ArchiverConstants.S3_BACKUP_KEY_PATTERN.matcher(summary.getKey());
                    if (!matcher.matches()) {
                        return null;
                    }
                    return matcher.group(1);
                })
                .filter(Objects::nonNull)
                .map(instantString -> ArchiverConstants.S3_BACKUP_KEY_DATE_TIME_FORMATTER.parse(instantString, Instant::from))
                .collect(Collectors.toList());
    }

    @Override
    public void uploadBackup(InputStream inputStream, Instant creationTime) throws UploadException {
        String key = String.format(ArchiverConstants.S3_BACKUP_KEY_FORMAT, ArchiverConstants.S3_BACKUP_KEY_DATE_TIME_FORMATTER.format(creationTime));

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


}
