package com.lantromipis.orchestration.util;

import java.sql.Connection;
import java.sql.Statement;

public class JdbcUtils {
    public static void closeJdbcConnectionSafely(Connection connection) {
        if (connection == null) {
            return;
        }

        try {
            connection.close();
        } catch (Exception ignored) {

        }
    }

    public static void closeJdbcStatementSafely(Statement statement) {
        if (statement == null) {
            return;
        }

        try {
            statement.close();
        } catch (Exception ignored) {

        }
    }

    private JdbcUtils() {
    }
}
