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

    private long timeline;
    private long startLsn;

    private Channel primaryChannel = null;
    private PgChannelSimpleQueryExecutorHandler queryExecutor = null;
    private PgStreamingReplicationHandler streamingReplicationHandler = null;

    private long lastWrittenToDiskLsnStart = LogSequenceNumberUtils.INVALID_LSN;
    private long lastWrittenToDiskLsnEnd = LogSequenceNumberUtils.INVALID_LSN;
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

            long startTime = System.currentTimeMillis();

            // hack to 'wait' until ChannelHandlerContext is added to handlers, because it is Async and sometimes causes NPE
            while ((!streamingReplicationHandler.isAdded() || !queryExecutor.isAdded()) && startTime + 10 < System.currentTimeMillis()) {
                log.debug("AWAITING");
            }

            //primaryChannel.pipeline().addFirst(new LoggingHandler(this.getClass(), LogLevel.DEBUG));

            extractTimelineAndLsn(forceUseLatestServerLsn);
            boolean successfullyStarted = startStreamingReplication();
            replicationActive.set(successfullyStarted);

            if (!successfullyStarted) {
                cleanUp();
                log.info("Failed to start continuous WAL archiving!");
            } else {
                log.info("Continuous WAL archiving started successfully!");
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
        log.info("Continuous WAL archiving stopped!");
    }

    private void cleanUp() {
        synchronized (this) {
            if (primaryChannel != null) {
                HandlerUtils.closeOnFlush(primaryChannel, ClientPostgresProtocolMessageEncoder.encodeClientTerminateMessage(primaryChannel.alloc()));
            }

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
            lastWrittenToDiskLsnStart = LogSequenceNumberUtils.INVALID_LSN;

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
                                postgresPersistedArchiverInfo.setWalSegmentSizeInBytes(postgresSettingsRuntimeProperties.getWalSegmentSizeInBytes());
                                raftFunctionalityCombinator.saveArchiverInfoInRaft(postgresPersistedArchiverInfo);
                                log.debug("Confirmed uploading WAL file with name {} to remote storage in Raft.", file.getName());
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
                                    log.warn("Interrupting WAL file {} uploading because executor service is shutdown", file.getName());
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
        if (!forceUseLatestServerLsn) {
            PostgresPersistedArchiverInfo postgresPersistedArchiverInfo = raftFunctionalityCombinator.getArchiveInfo();
            if (postgresPersistedArchiverInfo != null && StringUtils.isNotEmpty(postgresPersistedArchiverInfo.getLastUploadedWal())) {
                if (postgresPersistedArchiverInfo.getWalSegmentSizeInBytes() != postgresSettingsRuntimeProperties.getWalSegmentSizeInBytes()) {
                    log.warn("Detected that previous WAL segment size was {} while new WAL segment size is {}. " +
                                    "This most likely means that WAL segment size was changed. " +
                                    "Will use latest server LSN and restart wal streaming! New backup creation is recommended!",
                            postgresPersistedArchiverInfo.getWalSegmentSizeInBytes(),
                            postgresSettingsRuntimeProperties.getWalSegmentSizeInBytes()
                    );
                } else {
                    long nextWalStartLsn = LogSequenceNumberUtils.getNextWalFileStartLsn(postgresPersistedArchiverInfo.getLastUploadedWal(), postgresSettingsRuntimeProperties.getWalSegmentSizeInBytes());
                    timeline = LogSequenceNumberUtils.extractTimelineFromWalFileName(postgresPersistedArchiverInfo.getLastUploadedWal());
                    startLsn = nextWalStartLsn;
                    return;
                }
            }
        }

        // extract using IDENTIFY_SYSTEM
        PgChannelSimpleQueryExecutorHandler.CommandExecutionResult identifySystemResult = queryExecutor.executeQueryBlocking(
                "IDENTIFY_SYSTEM",
                -1
        );

        switch (identifySystemResult.getStatus()) {
            case SUCCESS -> {
                try {
                    PgResultSet resultSet = DecoderUtils.extractResultSetFromMessages(identifySystemResult.getMessageInfos());
                    PgResultSet.PgRow row = resultSet.getRow(0);

                    String timelineStr = new String(row.getCellValueByName(PostgresProtocolStreamingReplicationConstants.IDENTIFY_SYSTEM_TIMELINE_COLUMN_NAME));
                    timeline = Long.parseLong(timelineStr, 16);

                    // extract first lsn of current WAL file
                    String serverFlushLsnStr = new String(row.getCellValueByName(PostgresProtocolStreamingReplicationConstants.IDENTIFY_SYSTEM_X_LOG_POS_COLUMN_NAME));
                    long serverFlushLsn = LogSequenceNumberUtils.stringToLsn(serverFlushLsnStr);
                    long serverFlushWalFileFirstLsn = LogSequenceNumberUtils.getFirstLsnInWalFileWithProvidedLsn(
                            serverFlushLsn,
                            postgresSettingsRuntimeProperties.getWalSegmentSizeInBytes()
                    );
                    startLsn = serverFlushWalFileFirstLsn;
                } catch (Exception e) {
                    log.error("Exception while trying to extract timeline and latest LSN from Postgres server!", e);
                    stopContinuousArchiving();
                }
            }
            case SERVER_ERROR ->
                    log.error("Unexpected server error while trying to execute IDENTIFY_SYSTEM query to start continuous WAL archiving! Message from server: {}",
                            PostgresErrorMessageUtils.getLoggableErrorMessageFromErrorResponse(identifySystemResult.getErrorResponse())
                    );
            case TIMEOUT ->
                    log.error("Timeout reached while trying to execute IDENTIFY_SYSTEM query to start continuous WAL archiving!");
            case CLIENT_ERROR ->
                    log.error("Unexpected client error while trying to execute IDENTIFY_SYSTEM query to start continuous WAL archiving!", identifySystemResult.getThrowable());
            default ->
                    log.error("Unexpected client error while trying to execute IDENTIFY_SYSTEM query to start continuous WAL archiving!");
        }

        if (!PgChannelSimpleQueryExecutorHandler.CommandExecutionResultStatus.SUCCESS.equals(identifySystemResult.getStatus())) {
            stopContinuousArchiving();
        }

    }

    private void replicationErrorCallback(PgStreamingReplicationHandler.ReplicationErrorResult result) {
        switch (result.getErrorType()) {
            case SERVER_ERROR -> log.error(
                    "Unexpected server error during continuous WAL archiving! Message from server: {}",
                    PostgresErrorMessageUtils.getLoggableErrorMessageFromErrorResponse(result.getErrorResponse())
            );
            case CLIENT_ERROR -> log.error("Unexpected error during continuous WAL archiving!", result.getThrowable());
        }
        stopContinuousArchiving();
    }

    private void replicationNewWalFragmentReceivedCallback(PgStreamingReplicationHandler.WalFragmentReceivedResult result) {
        try {
            boolean newFragmentBelongsToSameWal = LogSequenceNumberUtils.compareIfBelongsToSameWal(
                    lastWrittenToDiskLsnStart,
                    result.getFragmentStartLsn(),
                    postgresSettingsRuntimeProperties.getWalSegmentSizeInBytes()
            );
            // if NOT belongs to same WAL, switch WAL
            if (!newFragmentBelongsToSameWal) {
                if (currentWalRandomAccessFile != null) {
                    currentWalRandomAccessFile.close();

                    long lastWrittenToDiskLsnEndTemp = lastWrittenToDiskLsnEnd;
                    uploadCompletedFile(currentWalFile)
                            .thenRun(
                                    () -> {
                                        if (streamingReplicationHandler != null) {
                                            streamingReplicationHandler.confirmProcessedLsn(lastWrittenToDiskLsnEndTemp);
                                            log.debug("Confirmed flushed/applied LSN {} because WAL with such LSN was uploaded.", LogSequenceNumberUtils.lsnToString(lastWrittenToDiskLsnEndTemp));
                                        }
                                    }
                            );
                }

                String walFileName = LogSequenceNumberUtils.getWalFileNameForLsn(
                        timeline,
                        result.getFragmentStartLsn(),
                        postgresSettingsRuntimeProperties.getWalSegmentSizeInBytes()
                );

                currentWalFile = new File(filesPathsProducer.getPostgresWalStreamReceiverDirectoryPath() + "/" + walFileName);
                currentWalRandomAccessFile = new RandomAccessFile(currentWalFile, "rw");
                currentWalRandomAccessFile.setLength(postgresSettingsRuntimeProperties.getWalSegmentSizeInBytes());
                currentWalRandomAccessFile.seek(0);

                log.debug("Switched streaming replication WAL file to {}", walFileName);
            }

            currentWalRandomAccessFile.write(result.getFragment(), 0, result.getFragmentLength());
            lastWrittenToDiskLsnStart = result.getFragmentStartLsn();
            lastWrittenToDiskLsnEnd = result.getFragmentEndLsn();
        } catch (Exception e) {
            log.error("Failed to save WAL fragment to file!", e);
            stopContinuousArchiving();
        }
    }

    private void replicationForTimelineCompletedCallback(PgStreamingReplicationHandler.StreamingCompletedResult result) {
        PgChannelSimpleQueryExecutorHandler.CommandExecutionResult timelineHistoryResult = queryExecutor.executeQueryBlocking(
                "TIMELINE_HISTORY " + result.getNextTimeline(),
                -1
        );

        switch (timelineHistoryResult.getStatus()) {
            case SUCCESS -> {
                try {
                    PgResultSet resultSet = DecoderUtils.extractResultSetFromMessages(timelineHistoryResult.getMessageInfos());
                    PgResultSet.PgRow row = resultSet.getRow(0);

                    String timelineFileName = new String(row.getCellValueByName(PostgresProtocolStreamingReplicationConstants.TIMELINE_HISTORY_FILENAME_COLUMN_NAME));
                    byte[] timelineFileContent = row.getCellValueByName(PostgresProtocolStreamingReplicationConstants.TIMELINE_HISTORY_CONTENT_COLUMN_NAME);

                    File timelineFile = new File(filesPathsProducer.getPostgresWalStreamReceiverDirectoryPath() + "/" + timelineFileName);

                    RandomAccessFile timelineRandomAccessFile = new RandomAccessFile(timelineFile, "rw");

                    timelineRandomAccessFile.seek(0);
                    timelineRandomAccessFile.write(timelineFileContent);
                    timelineRandomAccessFile.close();

                    uploadCompletedFile(timelineFile);
                    // restart replication
                    startStreamingReplication();
                } catch (Exception e) {
                    log.error("Error while trying to write timeline history file!", e);
                    stopContinuousArchiving();
                }
            }
            case SERVER_ERROR -> log.error(
                    "Unexpected server error while trying to execute TIMELINE_HISTORY query to continue continuous WAL archiving! Message from server: {}",
                    PostgresErrorMessageUtils.getLoggableErrorMessageFromErrorResponse(timelineHistoryResult.getErrorResponse())
            );
            case TIMEOUT ->
                    log.error("Timeout reached while trying to execute TIMELINE_HISTORY query to start continue WAL archiving!");
            case CLIENT_ERROR ->
                    log.error("Unexpected client error while trying to execute TIMELINE_HISTORY query to continue WAL archiving!", timelineHistoryResult.getThrowable());
            default ->
                    log.error("Unexpected client error while trying to execute TIMELINE_HISTORY query to continue WAL archiving!");
        }

        if (!PgChannelSimpleQueryExecutorHandler.CommandExecutionResultStatus.SUCCESS.equals(timelineHistoryResult.getStatus())) {
            stopContinuousArchiving();
        }
    }

    private void postgresTimeout() {
        stopContinuousArchiving();
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
                this::replicationForTimelineCompletedCallback,
                this::postgresTimeout
        );

        try {
            boolean awaitSuccess = streamingStartLatch.await(archivingProperties.walStreaming().initialDelay().toMillis(), TimeUnit.MILLISECONDS);
            if (!awaitSuccess && !success.get()) {
                success.set(false);
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
            return false;
        }

        return success.get();
    }
}
