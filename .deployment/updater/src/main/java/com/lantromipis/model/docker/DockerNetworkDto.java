package com.lantromipis.model.docker;

import lombok.Data;

@Data
public class DockerNetworkDto {
    private String networkName;
    private boolean create;
}
