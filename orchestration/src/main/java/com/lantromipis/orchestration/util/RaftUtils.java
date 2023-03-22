package com.lantromipis.orchestration.util;

import com.lantromipis.configuration.properties.constant.PgFacadeConstants;
import com.lantromipis.orchestration.model.PgFacadeRaftNodeInfo;
import org.apache.ratis.protocol.RaftPeer;

import javax.enterprise.context.ApplicationScoped;
import java.time.Instant;

@ApplicationScoped
public class RaftUtils {
    public String getRpcAddress(String ipAddress, int port) {
        String ipAddressWithoutMask = ipAddress.replaceAll("\\/.*", "");
        return ipAddressWithoutMask + ":" + port;
    }

    public int getPriorityBasedOnTime(Instant instant) {
        int diff = (int) instant.minusMillis(PgFacadeConstants.DEFAULT_INSTANT_TO_COUNT_FROM.toEpochMilli()).getEpochSecond();
        return Integer.MAX_VALUE - diff;
    }
}
