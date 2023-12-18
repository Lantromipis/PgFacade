package com.lantromipis.postgresprotocol.model.protocol;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RowDescription {
    private List<FieldDescription> fieldDescriptions;

    @Data
    @Builder
    @AllArgsConstructor
    @NoArgsConstructor
    public static class FieldDescription {
        String fieldName;
        int tableOid;
        short columnAttributeNumber;
        int fieldDataTypeOid;
        short fieldDataTypeSize;
        int typeModifier;
        short formatCode;
    }
}
