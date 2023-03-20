package com.lantromipis.orchestration.service.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.statemachine.impl.BaseStateMachine;

import javax.enterprise.context.ApplicationScoped;

@Slf4j
@ApplicationScoped
public class CustomRaftStateMachine extends BaseStateMachine {

    @Override
    public void notifyLeaderChanged(RaftGroupMemberId groupMemberId, RaftPeerId newLeaderId) {
        log.info("LEADER CHANGED TO ID: {} SELF ID: {}", newLeaderId.toString(), getId());
        super.notifyLeaderChanged(groupMemberId, newLeaderId);
    }

    
}
