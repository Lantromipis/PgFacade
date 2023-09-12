package com.lantromipis.rest.model.api.shutdown;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ShutdownMessageResponseDto {
    private String message;
}
