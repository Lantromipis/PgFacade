package com.lantromipis.model.copy;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BalancerHostInFileInfoCopy {
    private String address;
    private int port;
}
