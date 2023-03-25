package com.lantromipis.pgfacadeprotocol.constant;

import com.lantromipis.pgfacadeprotocol.message.*;

public class MessageMarkerConstants {

    public static final byte UNKNOWN_MESSAGE_MARKER = '*';
    public static final byte REJECT_MESSAGE_MARKER = '!';
    public static final byte HELLO_REQUEST_MESSAGE_MARKER = 'A';
    public static final byte HELLO_RESPONSE_MESSAGE_MARKER = 'B';
    public static final byte VOTE_REQUEST_MESSAGE_MARKER = 'C';
    public static final byte VOTE_RESPONSE_MESSAGE_MARKER = 'D';
    public static final byte APPEND_REQUEST_MESSAGE_MARKER = 'E';
    public static final byte APPEND_RESPONSE_MESSAGE_MARKER = 'F';

    public static Class<? extends AbstractMessage> getMessageClassByMarker(byte marker) {
        switch (marker) {
            case APPEND_REQUEST_MESSAGE_MARKER -> {
                return AppendRequest.class;
            }
            case HELLO_REQUEST_MESSAGE_MARKER -> {
                return HelloRequest.class;
            }
            case HELLO_RESPONSE_MESSAGE_MARKER -> {
                return HelloResponse.class;
            }
            case VOTE_REQUEST_MESSAGE_MARKER -> {
                return VoteRequest.class;
            }
            case VOTE_RESPONSE_MESSAGE_MARKER -> {
                return VoteResponse.class;
            }
            default -> {
                return UnknownMessage.class;
            }
        }
    }

    public MessageMarkerConstants() {
    }
}
