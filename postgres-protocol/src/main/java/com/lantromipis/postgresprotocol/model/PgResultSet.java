package com.lantromipis.postgresprotocol.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.LinkedHashMap;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PgResultSet {
    private List<PgRow> rows;

    public PgRow getRow(int idx) {
        return rows.get(idx);
    }

    public static class PgRow {
        private LinkedHashMap<String, byte[]> columns;

        public PgRow(LinkedHashMap<String, byte[]> columns) {
            this.columns = columns;
        }

        public byte[] getCellValueByName(String name) {
            return columns.get(name);
        }

        public String getCellValueByNameAsString(String name) {
            byte[] value = columns.get(name);

            if (value == null) {
                return null;
            }

            return new String(value);
        }

        public String getCellValueByIdxAsString(int idx) {
            byte[] value = getCellValueByIdx(idx);

            if (value == null) {
                return null;
            }

            return new String(value);
        }

        public byte[] getCellValueByIdx(int idx) {
            String key = null;
            int counter = 0;

            for (String columnName : columns.sequencedKeySet()) {
                if (counter == idx) {
                    key = columnName;
                    break;
                }
                counter++;
            }

            if (key == null) {
                return null;
            }

            return columns.get(key);
        }
    }
}
