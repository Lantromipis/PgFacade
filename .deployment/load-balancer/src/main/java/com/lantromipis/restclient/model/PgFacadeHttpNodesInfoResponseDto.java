package com.lantromipis.restclient.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PgFacadeHttpNodesInfoResponseDto {
    private List<PgFacadeNodeHttpConnectionInfo> httpNodesInfo;
}
