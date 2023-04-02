package com.lantromipis.pgfacadeprotocol.utils;

import com.lantromipis.pgfacadeprotocol.message.*;
import io.netty.buffer.ByteBuf;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

import static com.lantromipis.pgfacadeprotocol.constant.MessageMarkerConstants.*;

@Getter
@Setter
@AllArgsConstructor
public class MessageEncoderUtils {
    public static void encodeMessage(AbstractMessage abstractMessage, ByteBuf target) {
        switch (abstractMessage.getMessageMarker()) {
            case REJECT_MESSAGE_MARKER -> {
                encodeRejectRequest((RejectResponse) abstractMessage, target);
            }
            case VOTE_REQUEST_MESSAGE_MARKER -> {
                encodeVoteRequestMessage((VoteRequest) abstractMessage, target);
            }
            case VOTE_RESPONSE_MESSAGE_MARKER -> {
                encodeVoteResponseMessage((VoteResponse) abstractMessage, target);
            }
            case APPEND_REQUEST_MESSAGE_MARKER -> {
                encodeAppendRequestMessage((AppendRequest) abstractMessage, target);
            }
            case APPEND_RESPONSE_MESSAGE_MARKER -> {
                encodeAppendResponseMessage((AppendResponse) abstractMessage, target);
            }
            case INSTALL_SNAPSHOT_REQUEST_MESSAGE_MARKER -> {
                encodeInstallSnapshotMessage((InstallSnapshotRequest) abstractMessage, target);
            }
            case INSTALL_SNAPSHOT_RESPONSE_MESSAGE_MARKER -> {
                encodeInstallSnapshotResponse((InstallSnapshotResponse) abstractMessage, target);
            }
            default -> {
                encodeUnknownMessage(target);
            }
        }
    }

    public static void encodeString(String string, ByteBuf target) {
        encodeByteArray(string == null ? null : string.getBytes(), target);
    }

    public static void encodeByteArray(byte[] bytes, ByteBuf target) {
        if (bytes == null || bytes.length == 0) {
            target.writeInt(0);
            return;
        }

        target.writeInt(bytes.length);
        target.writeBytes(bytes);
    }

    private static void encodeUnknownMessage(ByteBuf target) {
        target.writeByte(UNKNOWN_MESSAGE_MARKER);
    }

    private static void encodeRejectRequest(RejectResponse rejectResponse, ByteBuf target) {
        encodeAbstractMessage(rejectResponse, target);

        encodeString(rejectResponse.getMessage(), target);
    }

    private static void encodeAbstractMessage(AbstractMessage abstractMessage, ByteBuf target) {
        target.writeByte(abstractMessage.getMessageMarker());

        encodeString(abstractMessage.getGroupId(), target);
        encodeString(abstractMessage.getNodeId(), target);
    }

    private static void encodeVoteRequestMessage(VoteRequest voteRequest, ByteBuf target) {
        encodeAbstractMessage(voteRequest, target);

        target.writeLong(voteRequest.getTerm());
        target.writeLong(voteRequest.getLastLogIndex());
        target.writeLong(voteRequest.getLastLogTerm());
        target.writeLong(voteRequest.getRound());
    }

    private static void encodeVoteResponseMessage(VoteResponse voteResponse, ByteBuf target) {
        encodeAbstractMessage(voteResponse, target);

        target.writeLong(voteResponse.getTerm());
        target.writeBoolean(voteResponse.isAgreed());
        target.writeLong(voteResponse.getRound());
    }

    private static void encodeAppendRequestMessage(AppendRequest appendRequest, ByteBuf target) {
        encodeAbstractMessage(appendRequest, target);

        target.writeLong(appendRequest.getCurrentTerm());
        target.writeLong(appendRequest.getPreviousLogIndex());
        target.writeLong(appendRequest.getPreviousTerm());
        target.writeLong(appendRequest.getLeaderCommit());

        int operationsCount = appendRequest.getOperations() == null
                ? 0
                : appendRequest.getOperations().size();

        target.writeInt(operationsCount);

        if (operationsCount > 0) {
            for (var operation : appendRequest.getOperations()) {
                target.writeLong(operation.getTerm());
                target.writeLong(operation.getIndex());
                encodeString(operation.getCommand(), target);
                encodeByteArray(operation.getData(), target);
            }
        }

        target.writeLong(appendRequest.getShrinkIndex());
    }

    private static void encodeAppendResponseMessage(AppendResponse appendResponse, ByteBuf target) {
        encodeAbstractMessage(appendResponse, target);

        target.writeLong(appendResponse.getTerm());
        target.writeBoolean(appendResponse.isSuccess());
        target.writeLong(appendResponse.getMatchIndex());
        target.writeLong(appendResponse.getCommitIndex());
    }

    private static void encodeInstallSnapshotMessage(InstallSnapshotRequest installSnapshotRequest, ByteBuf target) {
        encodeAbstractMessage(installSnapshotRequest, target);

        target.writeLong(installSnapshotRequest.getTerm());
        encodeString(installSnapshotRequest.getLeaderId(), target);
        target.writeLong(installSnapshotRequest.getLastIncludedIndex());
        target.writeLong(installSnapshotRequest.getLeaderCommit());

        int operationsCount = installSnapshotRequest.getChunks() == null
                ? 0
                : installSnapshotRequest.getChunks().size();

        target.writeInt(operationsCount);

        if (operationsCount > 0) {
            for (var chunk : installSnapshotRequest.getChunks()) {
                encodeString(chunk.getChunkName(), target);
                encodeByteArray(chunk.getData(), target);
            }
        }
    }

    public static void encodeInstallSnapshotResponse(InstallSnapshotResponse installSnapshotResponse, ByteBuf target) {
        encodeAbstractMessage(installSnapshotResponse, target);

        target.writeLong(installSnapshotResponse.getTerm());
        target.writeBoolean(installSnapshotResponse.isSuccess());
        target.writeLong(installSnapshotResponse.getLastIndex());
    }
}
