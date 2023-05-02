package com.lantromipis.rest.model.stats;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PgFacadeNodesInfoResponseDto {
    private List<PgFacadeNodeExternalConnectionInfoDto> nodesInfo;
}
