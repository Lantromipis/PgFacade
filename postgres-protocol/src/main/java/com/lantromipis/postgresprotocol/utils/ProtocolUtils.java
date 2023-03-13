package com.lantromipis.postgresprotocol.utils;

import com.lantromipis.postgresprotocol.constant.PostgresProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.encoder.ServerPostgresProtocolMessageEncoder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import javax.enterprise.context.ApplicationScoped;
import java.util.LinkedHashMap;
import java.util.Map;

@ApplicationScoped
public class ProtocolUtils {

    //TODO move to config
    public Map<String, String> serverParameters = new LinkedHashMap<>() {{
        //put("application_name", "\0\0");
        //put("client_encoding", "UTF8");
        //put("DateStyle", "ISO, DMY");
        //put("default_transaction_read_only", "off");
        //put("in_hot_standby", "off");
        //put("IntervalStyle", "postgres");
        //put("is_superuser", "off");
        //put("server_encoding", "UTF8");
        put("server_version", "15.1");
        //put("session_authorization", "postgres");
        //put("standard_confirming_strings", "on");
        //put("TimeZone", "UTC");
    }};

    public ByteBuf getServerParametersStatusMessage() {
        ByteBuf result = Unpooled.buffer();

        for (var entry : serverParameters.entrySet()) {
            result.writeBytes(ServerPostgresProtocolMessageEncoder.encodeParameterStatusMessage(entry.getKey(), entry.getValue()));
        }

        return result;
    }

    public static boolean checkIfMessageIsTermination(ByteBuf buf) {
        byte startChar = buf.readByte();
        buf.resetReaderIndex();

        return startChar == PostgresProtocolGeneralConstants.CLIENT_TERMINATION_MESSAGE_START_CHAR;
    }
}
