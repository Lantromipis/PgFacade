package com.lantromipis.orchestration.service.impl;

import com.lantromipis.configuration.model.PgFacadeRaftRole;
import com.lantromipis.configuration.properties.runtime.PgFacadeRuntimeProperties;
import com.lantromipis.orchestration.service.api.PgFacadeOrchestrator;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.eclipse.microprofile.context.ManagedExecutor;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.Objects;

@Slf4j
@ApplicationScoped
public class CustomRaftStateMachine extends BaseStateMachine {

    @Inject
    PgFacadeRuntimeProperties pgFacadeRuntimeProperties;

    @Inject
    PgFacadeOrchestrator pgFacadeOrchestrator;

    @Inject
    ManagedExecutor managedExecutor;

    @Override
    public void notifyLeaderChanged(RaftGroupMemberId groupMemberId, RaftPeerId newLeaderId) {
        log.info("Raft leader changed to node with id: '{}' self id: '{}'", newLeaderId.toString(), getId());

        if (Objects.equals(newLeaderId, getId())) {
            pgFacadeRuntimeProperties.setRaftRole(PgFacadeRaftRole.LEADER);
            log.info("This PgFacade node became leader. Starting orchestration...");
            managedExecutor.runAsync(() -> pgFacadeOrchestrator.startOrchestration());
        }
        super.notifyLeaderChanged(groupMemberId, newLeaderId);
    }
}
