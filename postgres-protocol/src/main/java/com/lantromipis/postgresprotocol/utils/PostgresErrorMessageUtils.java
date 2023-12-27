package com.lantromipis.postgresprotocol.utils;

import com.lantromipis.postgresprotocol.encoder.ServerPostgresProtocolMessageEncoder;
import com.lantromipis.postgresprotocol.model.protocol.ErrorResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.commons.lang3.StringUtils;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.lantromipis.postgresprotocol.constant.PostgresProtocolErrorAndNoticeConstant.*;

public class PostgresErrorMessageUtils {

    public static String getLoggableErrorMessageFromErrorResponse(ErrorResponse errorResponse) {
        if (errorResponse == null) {
            return null;
        }

        if (errorResponse.getMessage() == null) {
            return "no message";
        }

        return errorResponse.getMessage();
    }

    public static ByteBuf getAuthFailedForUserErrorMessage(String username, ByteBufAllocator allocator) {
        if (StringUtils.isEmpty(username)) {
            username = StringUtils.EMPTY;
        }

        Map<Byte, String> map = fatalErrorTemplate();

        map.put(SQLSTATE_CODE_MARKER, INVALID_PASSWORD_SQLSTATE_ERROR_CODE);
        map.put(MESSAGE_MARKER, String.format(AUTH_FAILED_MESSAGE_FORMAT, username));

        return ServerPostgresProtocolMessageEncoder.createErrorMessage(map, allocator);
    }

    public static ByteBuf getTooManyConnectionsErrorMessage(ByteBufAllocator allocator) {
        Map<Byte, String> map = fatalErrorTemplate();

        map.put(SQLSTATE_CODE_MARKER, TOO_MANY_CONNECTIONS_SQLSTATE_ERROR_CODE);
        map.put(MESSAGE_MARKER, TOO_MANY_CONNECTIONS);

        return ServerPostgresProtocolMessageEncoder.createErrorMessage(map, allocator);
    }

    private static Map<Byte, String> fatalErrorTemplate() {
        Map<Byte, String> map = new LinkedHashMap<>();

        map.put(SEVERITY_LOCALIZED_MARKER, FATAL_SEVERITY);
        map.put(SEVERITY_NOT_LOCALIZED_MARKER, FATAL_SEVERITY);

        return map;
    }
}
