package com.lantromipis.usermanagement.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;

/**
 * pg_shadow table row model
 * <p>
 * <a href="https://www.postgresql.org/docs/current/view-pg-shadow.html">...</a>
 */

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PgShadowTableRow {
    private String username;
    private String passwd;
    private LocalDate valUntil;
}
