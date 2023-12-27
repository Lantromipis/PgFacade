package com.lantromipis.orchestration.service.impl;

import com.lantromipis.configuration.producers.FilesPathsProducer;
import com.lantromipis.configuration.producers.RuntimePostgresConnectionProducer;
import com.lantromipis.configuration.properties.predefined.ArchivingProperties;
import com.lantromipis.configuration.properties.runtime.PostgresSettingsRuntimeProperties;
import com.lantromipis.orchestration.adapter.api.ArchiverStorageAdapter;
import com.lantromipis.orchestration.exception.PostgresContinuousArchivingException;
import com.lantromipis.orchestration.model.raft.PostgresPersistedArchiverInfo;
import com.lantromipis.orchestration.service.api.PostgresContinuousArchivingService;
import com.lantromipis.orchestration.util.RaftFunctionalityCombinator;
import com.lantromipis.postgresprotocol.constant.PostgresProtocolStreamingReplicationConstants;
import com.lantromipis.postgresprotocol.encoder.ClientPostgresProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.handler.frontend.PgChannelSimpleQueryExecutorHandler;
import com.lantromipis.postgresprotocol.handler.frontend.PgStreamingReplicationHandler;
import com.lantromipis.postgresprotocol.model.PgResultSet;
import com.lantromipis.postgresprotocol.utils.DecoderUtils;
import com.lantromipis.postgresprotocol.utils.HandlerUtils;
import com.lantromipis.postgresprotocol.utils.LogSequenceNumberUtils;
import com.lantromipis.postgresprotocol.utils.PostgresErrorMessageUtils;
import io.netty.channel.Channel;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@ApplicationScoped
public class PostgresContinuousArchivingServiceImpl implements PostgresContinuousArchivingService {

    @Inject
    RaftFunctionalityCombinator raftFunctionalityCombinator;

    @Inject
    FilesPathsProducer filesPathsProducer;

    @Inject
    RuntimePostgresConnectionProducer runtimePostgresConnectionProducer;

    @Inject
    PostgresSettingsRuntimeProperties postgresSettingsRuntimeProperties;

    @Inject
    Instance<ArchiverStorageAdapter> archiverAdapter;

    @Inject
    ArchivingProperties archivingProperties;

    private final AtomicBoolean replicationActive = new AtomicBoolean(false);

    private String timeline;
    private String startLsn;

    private Channel primaryChannel = null;
    private PgChannelSimpleQueryExecutorHandler queryExecutor = null;
    private PgStreamingReplicationHandler streamingReplicationHandler = null;

    private long lastWrittenToDiskLsn = LogSequenceNumberUtils.INVALID_LSN;
    private RandomAccessFile currentWalRandomAccessFile = null;
    private File currentWalFile = null;

    private ExecutorService walUploaderExecutor = null;


    public boolean isContinuousArchivingActive() {
        return replicationActive.get();
    }

    public boolean startContinuousArchiving(boolean forceUseLatestServerLsn) {
        // do not restart if already running
        if (replicationActive.get()) {
            return true;
        }

        try {
            primaryChannel = runtimePostgresConnectionProducer.createNewNettyChannelToPrimaryForReplication();
            walUploaderExecutor = Executors.newSingleThreadExecutor();

            queryExecutor = new PgChannelSimpleQueryExecutorHandler();
            primaryChannel.pipeline().addLast(queryExecutor);

            streamingReplicationHandler = new PgStreamingReplicationHandler();
            primaryChannel.pipeline().addLast(streamingReplicationHandler);

            extractTimelineAndLsn(forceUseLatestServerLsn);
            boolean successfullyStarted = startStreamingReplication();
            replicationActive.set(successfullyStarted);

            if (!successfullyStarted) {
                cleanUp();
            }
            return successfullyStarted;
        } catch (Exception e) {
            log.error("Error while initiating replication stream for WAL.", e);
            cleanUp();
            return false;
        }
    }

    public void stopContinuousArchiving() {
        replicationActive.set(false);
        cleanUp();
    }

    private void cleanUp() {
        synchronized (this) {
            HandlerUtils.closeOnFlush(primaryChannel, ClientPostgresProtocolMessageEncoder.encodeClientTerminateMessage(primaryChannel.alloc()));

            primaryChannel = null;
            queryExecutor = null;
            streamingReplicationHandler = null;

            if (currentWalRandomAccessFile != null) {
                try {
                    currentWalRandomAccessFile.close();
                } catch (IOException e) {
                    log.error("Failed to close WAL file!", e);
                }
            }

            currentWalRandomAccessFile = null;
            currentWalFile = null;
            lastWrittenToDiskLsn = LogSequenceNumberUtils.INVALID_LSN;

            if (walUploaderExecutor != null) {
                walUploaderExecutor.shutdownNow();
            }
            walUploaderExecutor = null;
        }
    }

    private CompletableFuture<Void> uploadCompletedFile(File file) {
        return CompletableFuture.runAsync(
                () -> {
                    ExecutorService executorService = walUploaderExecutor;
                    // retry each file until success
                    while (true) {
                        try {
                            raftFunctionalityCombinator.testIfAbleToCommitToRaft();
                            archiverAdapter.get().uploadWalFile(file);

                            if (LogSequenceNumberUtils.isWalFileName(file.getName())) {
                                PostgresPersistedArchiverInfo postgresPersistedArchiverInfo = raftFunctionalityCombinator.getArchiveInfo();
                                postgresPersistedArchiverInfo.setLastUploadedWal(file.getName());
                                raftFunctionalityCombinator.saveArchiverInfoInRaft(postgresPersistedArchiverInfo);
                                log.debug("Updated archiver info in Raft for file {}.", file.getName());
                            } else {
                                log.debug("Will not update archiver info in Raft for file {} because it is not WAL file.", file.getName());
                            }

                            file.delete();
                            // successfully uploaded file
                            break;
                        } catch (Throwable t) {
                            log.error("Failed to upload completed WAL file {} to storage! Can not upload any WAL futher to guarantee WAL file order! Will retry in {} ms..", file.getName(), 1000, t);
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException interruptedException) {
                                // interrupt only if executor service is shutdown
                                if (executorService == null || executorService.isShutdown()) {
                                    Thread.currentThread().interrupt();
                                    break;
                                }
                            }
                        }
                        // check if executor service is shutdown. If so, stop attempts to upload file
                        if (executorService == null || executorService.isShutdown()) {
                            break;
                        }
                    }
                },
                walUploaderExecutor
        );
    }

    private void extractTimelineAndLsn(boolean forceUseLatestServerLsn) throws PostgresContinuousArchivingException {
        PostgresPersistedArchiverInfo postgresPersistedArchiverInfo = raftFunctionalityCombinator.getArchiveInfo();

        if (!forceUseLatestServerLsn && postgresPersistedArchiverInfo != null && StringUtils.isNotEmpty(postgresPersistedArchiverInfo.getNextWal())) {
            timeline = LogSequenceNumberUtils.extractTimelineFromWalFileName(postgresPersistedArchiverInfo.getNextWal());
            startLsn = LogSequenceNumberUtils.getWalFileFirstLsnAsString(postgresPersistedArchiverInfo.getNextWal());
        } else {
            PgChannelSimpleQueryExecutorHandler.CommandExecutionResult identifySystemResult = queryExecutor.executeQueryBlocking(
                    "IDENTIFY_SYSTEM",
                    -1
            );

            switch (identifySystemResult.getStatus()) {
                case SUCCESS -> {
                    PgResultSet resultSet = DecoderUtils.extractResultSetFromMessages(identifySystemResult.getMessageInfos());
                    PgResultSet.PgRow row = resultSet.getRow(0);

                    timeline = new String(row.getCellValueByName(PostgresProtocolStreamingReplicationConstants.IDENTIFY_SYSTEM_TIMELINE_COLUMN_NAME));

                    // extract first lsn of current WAL file
                    String serverFlushLsnStr = new String(row.getCellValueByName(PostgresProtocolStreamingReplicationConstants.IDENTIFY_SYSTEM_X_LOG_POS_COLUMN_NAME));
                    long serverFlushLsn = LogSequenceNumberUtils.StringToLsn(serverFlushLsnStr);
                    long serverFlushWalFileFirstLsn = LogSequenceNumberUtils.getFirstLsnInWalFileWithProvidedLsn(serverFlushLsn);
                    startLsn = LogSequenceNumberUtils.lsnToString(serverFlushWalFileFirstLsn);
                }
                case SERVER_ERROR ->
                        log.error("Unexpected server error while trying to execute IDENTIFY_SYSTEM query to start continuous WAL archiving! Message from server: {}",
                                PostgresErrorMessageUtils.getLoggableErrorMessageFromErrorResponse(identifySystemResult.getErrorResponse())
                        );
                case TIMEOUT ->
                        log.error("Timeout reached while trying to execute IDENTIFY_SYSTEM query to start continuous WAL archiving!");
                default ->
                        log.error("Unexpected client error while trying to execute IDENTIFY_SYSTEM query to start continuous WAL archiving!");
            }
        }
    }

    private void replicationErrorCallback(PgStreamingReplicationHandler.ReplicationErrorResult result) {
        switch (result.getErrorType()) {
            case SERVER_ERROR ->
                    log.error("Unexpected server error during continuous WAL archiving! Message from server: {}",
                            PostgresErrorMessageUtils.getLoggableErrorMessageFromErrorResponse(result.getErrorResponse())
                    );
            case CLIENT_ERROR -> log.error("Unexpected error during continuous WAL archiving!", result.getThrowable());
        }
    }

    private void replicationNewWalFragmentReceivedCallback(PgStreamingReplicationHandler.WalFragmentReceivedResult result) {
        try {
            if (!LogSequenceNumberUtils.compareIfBelongsToSameWal(lastWrittenToDiskLsn, result.getFragmentStartLsn())) {
                if (currentWalRandomAccessFile != null) {
                    currentWalRandomAccessFile.close();

                    long lastFileLsn = lastWrittenToDiskLsn;
                    uploadCompletedFile(currentWalFile)
                            .thenRun(
                                    () -> {
                                        if (streamingReplicationHandler != null) {
                                            streamingReplicationHandler.confirmProcessedLsn(lastFileLsn);
                                            log.debug("Confirmed uploading LSN {}. Current LSN is {}", lastFileLsn, lastWrittenToDiskLsn);
                                        }
                                    }
                            );
                }

                String walFileName = LogSequenceNumberUtils.getWalFileNameForLsn(result.getFragmentStartLsn(), timeline);

                currentWalFile = new File(filesPathsProducer.getPostgresWalStreamReceiverDirectoryPath() + "/" + walFileName);
                currentWalRandomAccessFile = new RandomAccessFile(currentWalFile, "rw");
                currentWalRandomAccessFile.setLength(postgresSettingsRuntimeProperties.getWalSegmentSizeInBytes());
                currentWalRandomAccessFile.seek(0);

                log.debug("Switched streaming replication WAL file to {}", walFileName);
            }

            currentWalRandomAccessFile.write(result.getFragment());
            lastWrittenToDiskLsn = result.getFragmentEndLsn();
        } catch (Exception e) {
            throw new PostgresContinuousArchivingException("Failed to save WAL fragment to file!", e);
        }
    }

    private void replicationForTimelineCompletedCallback(PgStreamingReplicationHandler.StreamingCompletedResult result) {
        PgChannelSimpleQueryExecutorHandler.CommandExecutionResult timelineHistoryResult = queryExecutor.executeQueryBlocking(
                "TIMELINE_HISTORY " + result.getNextTimeline(),
                -1
        );

        switch (timelineHistoryResult.getStatus()) {
            case SUCCESS -> {
                PgResultSet resultSet = DecoderUtils.extractResultSetFromMessages(timelineHistoryResult.getMessageInfos());
                PgResultSet.PgRow row = resultSet.getRow(0);

                String timelineFileName = new String(row.getCellValueByName(PostgresProtocolStreamingReplicationConstants.TIMELINE_HISTORY_FILENAME_COLUMN_NAME));
                byte[] timelineFileContent = row.getCellValueByName(PostgresProtocolStreamingReplicationConstants.TIMELINE_HISTORY_CONTENT_COLUMN_NAME);

                File timelineFile = new File(filesPathsProducer.getPostgresWalStreamReceiverDirectoryPath() + "/" + timelineFileName);
                try {
                    RandomAccessFile timelineRandomAccessFile = new RandomAccessFile(timelineFile, "rw");

                    timelineRandomAccessFile.seek(0);
                    timelineRandomAccessFile.write(timelineFileContent);
                    timelineRandomAccessFile.close();

                    uploadCompletedFile(timelineFile);
                } catch (IOException e) {
                    throw new PostgresContinuousArchivingException("Error while trying to write timeline history file!");
                }
            }
            case SERVER_ERROR -> {
                String serverMessage = "no message";
                if (timelineHistoryResult.getErrorResponse() != null && timelineHistoryResult.getErrorResponse().getMessage() != null) {
                    serverMessage = timelineHistoryResult.getErrorResponse().getMessage();
                }
                throw new PostgresContinuousArchivingException("Unexpected server error while trying to execute TIMELINE_HISTORY query to continue continuous WAL archiving! " +
                        "Message from server: " + serverMessage);
            }
            case TIMEOUT ->
                    throw new PostgresContinuousArchivingException("Timeout reached while trying to execute TIMELINE_HISTORY query to start continue WAL archiving!");
            default ->
                    throw new PostgresContinuousArchivingException("Unexpected client error while trying to execute TIMELINE_HISTORY query to start continue WAL archiving!");
        }
    }

    private boolean startStreamingReplication() {
        CountDownLatch streamingStartLatch = new CountDownLatch(1);
        AtomicBoolean success = new AtomicBoolean(false);

        streamingReplicationHandler.startPhysicalReplication(
                null,
                startLsn,
                timeline,
                archivingProperties.walStreaming().keepaliveInterval().toMillis(),
                startedCallback -> {
                    switch (startedCallback.getStatus()) {
                        case SUCCESS -> success.set(true);
                        case SERVER_ERROR ->
                                log.error("Failed to start streaming replication due to error response from Postgres! Error message: {}",
                                        PostgresErrorMessageUtils.getLoggableErrorMessageFromErrorResponse(startedCallback.getErrorResponse())
                                );
                        case CLIENT_ERROR ->
                                log.error("Failed to start streaming replication due to client error!", startedCallback.getThrowable());
                    }

                    streamingStartLatch.countDown();
                },
                this::replicationErrorCallback,
                this::replicationNewWalFragmentReceivedCallback,
                this::replicationForTimelineCompletedCallback
        );

        try {
            success.set(streamingStartLatch.await(archivingProperties.walStreaming().initialDelay().toMillis(), TimeUnit.MILLISECONDS));
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
            return false;
        }

        return success.get();
    }
}
