package com.lantromipis.model.docker;

import lombok.Data;

@Data
public class DockerResourcesSettings {
    private Long cpuLimit;
    private Long memoryLimit;
}
