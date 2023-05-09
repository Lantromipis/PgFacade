package com.lantromipis.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConfigurationInfo {
    private String pgFacadeUsername;
    private String pgFacadePassword;
    private String pgFacadeDatabase;

    private boolean createReplicationUser;
    private String replicationUsername;
    private String replicationPassword;
}
