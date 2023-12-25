package com.lantromipis.postgresprotocol.model.protocol;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Objects;

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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            FieldDescription that = (FieldDescription) o;

            if (tableOid != that.tableOid) return false;
            return Objects.equals(fieldName, that.fieldName);
        }

        @Override
        public int hashCode() {
            int result = fieldName != null ? fieldName.hashCode() : 0;
            result = 31 * result + tableOid;
            return result;
        }
    }
}
