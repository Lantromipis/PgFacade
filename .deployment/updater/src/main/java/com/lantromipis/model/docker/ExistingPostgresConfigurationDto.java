package com.lantromipis.model.docker;

import lombok.Data;

@Data
public class ExistingPostgresConfigurationDto {
    private boolean configurePostgres;
    private PostgresCredentialsDto superuserCredentials;

    private String pgFacadeUsername;
    private String pgFacadePassword;
    private String pgFacadeDatabase;

    private boolean createReplicationUser;
    private String replicationUsername;
    private String replicationPassword;
}
