package com.lantromipis.model;

import lombok.Data;

@Data
public class NewPostgresConfigurationDto {
    private String pgFacadeUsername;
    private String pgFacadePassword;
    private String pgFacadeDatabase;

    private boolean createReplicationUser;
    private String replicationUsername;
    private String replicationPassword;
}
