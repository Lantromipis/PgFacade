package com.lantromipis.orchestration.adapter.impl;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.lantromipis.configuration.properties.predefined.ArchivingProperties;
import com.lantromipis.orchestration.adapter.api.ArchiverAdapter;
import io.quarkus.arc.lookup.LookupIfProperty;
import lombok.extern.slf4j.Slf4j;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@Slf4j
@ApplicationScoped
@LookupIfProperty(name = "pg-facade.archiving.adapter", stringValue = "s3")
public class S3ArchiverAdapter implements ArchiverAdapter {

    @Inject
    ArchivingProperties archivingProperties;

    private AmazonS3 s3client;

    @Override
    public void initialize() {
        log.info("Initializing S3 archiving adapter.");

        ArchivingProperties.S3ArchiverProperties s3ArchiverProperties = archivingProperties.s3();

        Protocol protocol =
                s3ArchiverProperties.protocol().equals(ArchivingProperties.ProtocolType.HTTP)
                        ? Protocol.HTTP
                        : Protocol.HTTPS;


        s3client = AmazonS3ClientBuilder
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

        s3client.listBuckets().forEach(bucket -> log.info(bucket.getName()));

        log.info("S3 archiving adapter initialization completed!");
    }
}
