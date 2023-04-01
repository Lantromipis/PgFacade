package com.lantromipis.configuration.producers;

import com.lantromipis.configuration.properties.constant.PgFacadeConstants;
import com.lantromipis.configuration.properties.predefined.OrchestrationProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

@Slf4j
@ApplicationScoped
public class FilesPathsProducer {
    @Inject
    OrchestrationProperties orchestrationProperties;

    // TODO implement by some identifier and adapter
    @PostConstruct
    public void createDirs() {
        try {
            Files.createDirectories(Paths.get(getPersistedPropertiesDirPath()));
            Files.createDirectories(Paths.get(getPostgresWalStreamReceiverDirectoryPath()));
            File raftDir = new File(getRaftDirPath());
            if (raftDir.exists()) {
                FileUtils.forceDelete(raftDir);
            }
            Files.createDirectories(Paths.get(getRaftDirPath()));
        } catch (IOException e) {
            log.error("Error while creating directories for PgFacade local files", e);
        }
    }

    private String getLocalFilesDirPath() {
        return orchestrationProperties.docker().pgFacade().localFilesDirectory();
    }

    private String getPersistedPropertiesDirPath() {
        return orchestrationProperties.docker().pgFacade().localFilesDirectory() + "/" + PgFacadeConstants.PG_FACADE_PERSISTED_PROPERTIES_DIR;
    }

    public String getRaftDirPath() {
        return "/tmp/" + PgFacadeConstants.PG_FACADE_RAFT_DIR;
    }

    public String getPostgresWalStreamReceiverDirectoryPath() {
        return getLocalFilesDirPath()
                + "/"
                + PgFacadeConstants.POSTGRES_WAL_STREAM_DIRECTORY_NAME;
    }

    public String getPostgresWalStreamUploaderDirectoryPath() {
        return getLocalFilesDirPath()
                + "/"
                + PgFacadeConstants.POSTGRES_WAL_UPLOAD_DIRECTORY_NAME;
    }

    public String getPostgresNodesInfosFilePath() {
        return getPersistedPropertiesDirPath()
                + "/"
                + PgFacadeConstants.POSTGRES_NODE_INFO_FILE_NAME;
    }

    public String getPostgresSettingsInfosFilePath() {
        return getPersistedPropertiesDirPath()
                + "/"
                + PgFacadeConstants.POSTGRES_SETTINGS_INFO_FILE_NAME;
    }

    public String getPostgresArchiveInfosFilePath() {
        return getPersistedPropertiesDirPath()
                + "/"
                + PgFacadeConstants.POSTGRES_ARCHIVE_INFO_FILE_NAME;
    }
}
