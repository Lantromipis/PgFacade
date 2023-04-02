package com.lantromipis.orchestration.model;

import com.lantromipis.orchestration.restclient.HealtcheckTemplateRestClient;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.concurrent.atomic.AtomicInteger;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PgFacadeInstanceInfo {
    private String raftIdAndAdapterId;
    private String address;
    private AtomicInteger unsuccessfulHealtcheckCount;
    private HealtcheckTemplateRestClient client;
}
