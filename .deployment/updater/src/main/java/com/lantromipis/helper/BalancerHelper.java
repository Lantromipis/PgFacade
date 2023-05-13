package com.lantromipis.helper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lantromipis.model.copy.BalancerHostInFileInfoCopy;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.List;

@ApplicationScoped
public class BalancerHelper {

    @Inject
    ObjectMapper objectMapper;

    public String hostFileContentForSingleHost(String address, int port) throws Exception {
        List<BalancerHostInFileInfoCopy> info = List.of(
                BalancerHostInFileInfoCopy
                        .builder()
                        .address(address)
                        .port(port)
                        .build()
        );

        return objectMapper.writeValueAsString(info);
    }

    public String getHostFilePath() {
        return "/deployments/hosts.json";
    }
}
