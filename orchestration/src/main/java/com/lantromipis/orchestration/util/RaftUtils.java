package com.lantromipis.orchestration.util;

import com.lantromipis.orchestration.model.PgFacadeRaftNodeInfo;
import org.apache.ratis.protocol.RaftPeer;

import javax.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class RaftUtils {
    public String getRpcAddress(String ipAddress, int port) {
        String ipAddressWithoutMask = ipAddress.replaceAll("\\/.*", "");
        return ipAddressWithoutMask + ":" + port;
    }
}
