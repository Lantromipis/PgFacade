package com.lantromipis.pgfacadeprotocol.utils;

import com.lantromipis.pgfacadeprotocol.message.*;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

import static com.lantromipis.pgfacadeprotocol.constant.MessageMarkerConstants.*;

public class MessageDecoderUtils {
    public static AbstractMessage decodeMessage(ByteBuf byteBuf) {
        byte marker = byteBuf.readByte();

        switch (marker) {
            case HELLO_REQUEST_MESSAGE_MARKER -> {
                return decodeHelloRequestMessage(byteBuf);
            }
            case HELLO_RESPONSE_MESSAGE_MARKER -> {
                return decodeHelloResponseMessage(byteBuf);
            }
            case VOTE_REQUEST_MESSAGE_MARKER -> {
                return decodeVoteRequestMessage(byteBuf);
            }
            case VOTE_RESPONSE_MESSAGE_MARKER -> {
                return decodeVoteResponseMessage(byteBuf);
            }
            case APPEND_REQUEST_MESSAGE_MARKER -> {
                return decodeAppendRequestMessage(byteBuf);
            }
            case APPEND_RESPONSE_MESSAGE_MARKER -> {
                return decodeAppendResponseMessage(byteBuf);
            }
            default -> {
                return UnknownMessage.builder().build();
            }
        }
    }

    public static String readString(ByteBuf byteBuf) {
        return new String(readByteArray(byteBuf));
    }

    public static byte[] readByteArray(ByteBuf byteBuf) {
        int arrayLength = byteBuf.readInt();
        if (arrayLength == 0) {
            return new byte[0];
        }

        byte[] bytes = new byte[arrayLength];
        byteBuf.readBytes(bytes, 0, arrayLength);

        return bytes;
    }

    private static HelloRequest decodeHelloRequestMessage(ByteBuf byteBuf) {
        return HelloRequest
                .builder()
                .groupId(readString(byteBuf))
                .nodeId(readString(byteBuf))
                .build();
    }

    private static HelloResponse decodeHelloResponseMessage(ByteBuf byteBuf) {
        return HelloResponse
                .builder()
                .groupId(readString(byteBuf))
                .nodeId(readString(byteBuf))
                .ack(byteBuf.readBoolean())
                .currentLeaderNodeId(readString(byteBuf))
                .build();
    }

    private static VoteRequest decodeVoteRequestMessage(ByteBuf byteBuf) {
        return VoteRequest
                .builder()
                .groupId(readString(byteBuf))
                .nodeId(readString(byteBuf))
                .term(byteBuf.readLong())
                .lastLogIndex(byteBuf.readLong())
                .lastLogTerm(byteBuf.readLong())
                .round(byteBuf.readInt())
                .build();
    }

    private static VoteResponse decodeVoteResponseMessage(ByteBuf byteBuf) {
        return VoteResponse
                .builder()
                .groupId(readString(byteBuf))
                .nodeId(readString(byteBuf))
                .term(byteBuf.readLong())
                .agreed(byteBuf.readBoolean())
                .round(byteBuf.readInt())
                .build();
    }

    private static List<AppendRequest.Operation> decodeOperations(ByteBuf byteBuf) {
        int operationsCount = byteBuf.readInt();
        List<AppendRequest.Operation> operations = new ArrayList<>(operationsCount);
        if (operationsCount > 0) {
            for (int i = 0; i < operationsCount; i++) {
                operations.add(
                        AppendRequest.Operation
                                .builder()
                                .term(byteBuf.readLong())
                                .index(byteBuf.readLong())
                                .command(readString(byteBuf))
                                .data(readByteArray(byteBuf))
                                .build()
                );
            }
        }

        return operations;
    }

    private static AppendRequest decodeAppendRequestMessage(ByteBuf byteBuf) {
        return AppendRequest
                .builder()
                .groupId(readString(byteBuf))
                .nodeId(readString(byteBuf))
                .currentTerm(byteBuf.readLong())
                .previousLogIndex(byteBuf.readLong())
                .previousTerm(byteBuf.readLong())
                .leaderCommit(byteBuf.readLong())
                .operations(decodeOperations(byteBuf))
                .build();
    }

    private static AppendResponse decodeAppendResponseMessage(ByteBuf byteBuf) {
        return AppendResponse
                .builder()
                .groupId(readString(byteBuf))
                .nodeId(readString(byteBuf))
                .term(byteBuf.readLong())
                .success(byteBuf.readBoolean())
                .matchIndex(byteBuf.readLong())
                .build();
    }

}
