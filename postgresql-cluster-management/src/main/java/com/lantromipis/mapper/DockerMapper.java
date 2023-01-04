package com.lantromipis.mapper;

import com.lantromipis.constant.DockerConstants;
import com.lantromipis.model.InstanceStatus;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class DockerMapper {
    public InstanceStatus from(String dockerContainerState) {
        return DockerConstants.ContainerState.RUNNING.getValue().equals(dockerContainerState)
                ? InstanceStatus.ACTIVE
                : InstanceStatus.NOT_ACTIVE;
    }
}
