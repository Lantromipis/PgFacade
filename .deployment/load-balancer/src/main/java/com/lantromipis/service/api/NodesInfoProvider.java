package com.lantromipis.service.api;

import com.lantromipis.model.PgFacadeNodeInfo;

import java.util.List;

public interface NodesInfoProvider {

    void reloadHosts();

    List<PgFacadeNodeInfo> getNodeInfos();
}
