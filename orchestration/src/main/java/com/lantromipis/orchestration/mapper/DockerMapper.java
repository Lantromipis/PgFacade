package com.lantromipis.orchestration.mapper;

import com.lantromipis.orchestration.constant.DockerConstants;
import com.lantromipis.orchestration.model.InstanceStatus;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class DockerMapper {
    public InstanceStatus from(String dockerContainerState) {
        return DockerConstants.ContainerState.RUNNING.getValue().equals(dockerContainerState)
                ? InstanceStatus.ACTIVE
                : InstanceStatus.NOT_ACTIVE;
    }
}
