package com.lantromipis.pgfacadeprotocol.utils;

import com.lantromipis.pgfacadeprotocol.model.api.RaftNode;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class InternalCommandsEncoderDecoder {
    public static byte[] encodeAddRaftNodeCommandData(RaftNode raftNode) {
        ByteBuf byteBuf = Unpooled.buffer();

        MessageEncoderUtils.encodeString(raftNode.getGroupId(), byteBuf);
        MessageEncoderUtils.encodeString(raftNode.getId(), byteBuf);
        MessageEncoderUtils.encodeString(raftNode.getIpAddress(), byteBuf);

        byteBuf.writeInt(raftNode.getPort());

        byte[] bytes = byteBuf.array();
        byteBuf.release();
        return bytes;
    }

    public static RaftNode decodeAddRaftNodeCommandData(byte[] bytes) {
        ByteBuf byteBuf = Unpooled.buffer();
        byteBuf.writeBytes(bytes);

        RaftNode raftNode = RaftNode
                .builder()
                .groupId(MessageDecoderUtils.readString(byteBuf))
                .id(MessageDecoderUtils.readString(byteBuf))
                .ipAddress(MessageDecoderUtils.readString(byteBuf))
                .port(byteBuf.readInt())
                .build();

        byteBuf.release();

        return raftNode;
    }
}
