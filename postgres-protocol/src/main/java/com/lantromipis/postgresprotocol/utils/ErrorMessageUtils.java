package com.lantromipis.postgresprotocol.utils;

import com.lantromipis.postgresprotocol.constant.PostgresProtocolErrorAndNoticeConstant;
import com.lantromipis.postgresprotocol.encoder.ServerPostgresProtocolMessageEncoder;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static com.lantromipis.postgresprotocol.constant.PostgresProtocolErrorAndNoticeConstant.*;

public class ErrorMessageUtils {
    public static ByteBuf getAuthFailedForUserErrorMessage(String username) {
        if (StringUtils.isEmpty(username)) {
            username = StringUtils.EMPTY;
        }

        Map<Byte, String> map = new LinkedHashMap<>();
        map.put(SEVERITY_LOCALIZED_MARKER, FATAL_SEVERITY);
        map.put(SEVERITY_NOT_LOCALIZED_MARKER, FATAL_SEVERITY);
        map.put(SQLSTATE_CODE_MARKER, INVALID_PASSWORD_SQLSTATE_ERROR_CODE);
        map.put(MESSAGE_MARKER, String.format(AUTH_FAILED_MESSAGE_FORMAT, username));

        return ServerPostgresProtocolMessageEncoder.createErrorMessage(map);
    }
}
