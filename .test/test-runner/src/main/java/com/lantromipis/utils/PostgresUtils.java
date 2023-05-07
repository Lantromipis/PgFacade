package com.lantromipis.utils;

public class PostgresUtils {

    public static String createJdbcUrl(String host, int port, String database) {
        return "jdbc:postgresql://"
                + host
                + ":"
                + port
                + "/"
                + database;
    }
}
