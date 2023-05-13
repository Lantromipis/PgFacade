package com.lantromipis.model.docker;

import lombok.Data;

@Data
public class PostgresCredentialsDto {
    private String superuserName;
    private String superuserPassword;
    private String superuserDatabase;
}
