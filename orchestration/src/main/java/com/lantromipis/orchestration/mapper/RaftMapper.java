package com.lantromipis.orchestration.mapper;

import com.lantromipis.orchestration.model.PgFacadeRaftNodeInfo;
import com.lantromipis.orchestration.util.RaftUtils;
import org.apache.ratis.protocol.RaftPeer;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
public class RaftMapper {

    @Inject
    RaftUtils raftUtils;

    public RaftPeer from(PgFacadeRaftNodeInfo nodeInfo) {
        return RaftPeer
                .newBuilder()
                .setId(nodeInfo.getPlatformAdapterIdentifier())
                .setAddress(raftUtils.getRpcAddress(nodeInfo.getAddress(), nodeInfo.getPort()))
                .setPriority(raftUtils.getPriorityBasedOnTime(nodeInfo.getCreatedWhen()))
                .build();
    }
}
