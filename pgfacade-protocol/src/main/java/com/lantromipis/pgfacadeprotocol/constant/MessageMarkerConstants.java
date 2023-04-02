package com.lantromipis.pgfacadeprotocol.constant;

public class MessageMarkerConstants {

    public static final byte UNKNOWN_MESSAGE_MARKER = 'A';
    public static final byte REJECT_MESSAGE_MARKER = 'B';
    public static final byte VOTE_REQUEST_MESSAGE_MARKER = 'C';
    public static final byte VOTE_RESPONSE_MESSAGE_MARKER = 'D';
    public static final byte APPEND_REQUEST_MESSAGE_MARKER = 'E';
    public static final byte APPEND_RESPONSE_MESSAGE_MARKER = 'F';
    public static final byte INSTALL_SNAPSHOT_REQUEST_MESSAGE_MARKER = 'G';
    public static final byte INSTALL_SNAPSHOT_RESPONSE_MESSAGE_MARKER = 'H';

    public MessageMarkerConstants() {
    }
}
