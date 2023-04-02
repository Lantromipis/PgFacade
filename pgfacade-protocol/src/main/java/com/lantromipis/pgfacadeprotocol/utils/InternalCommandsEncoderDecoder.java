package com.lantromipis.pgfacadeprotocol.utils;

import com.lantromipis.pgfacadeprotocol.model.api.RaftNode;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.ArrayList;
import java.util.List;

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

    public static byte[] encodeUpdateRaftMembershipCommandData(List<RaftNode> raftNodes) {
        ByteBuf byteBuf = Unpooled.buffer();

        byteBuf.writeInt(raftNodes.size());

        for (RaftNode raftNode : raftNodes) {
            MessageEncoderUtils.encodeString(raftNode.getGroupId(), byteBuf);
            MessageEncoderUtils.encodeString(raftNode.getId(), byteBuf);
            MessageEncoderUtils.encodeString(raftNode.getIpAddress(), byteBuf);

            byteBuf.writeInt(raftNode.getPort());
        }

        byte[] bytes = byteBuf.array();
        byteBuf.release();
        return bytes;
    }

    public static List<RaftNode> decodeUpdateRaftMembershipCommandData(byte[] bytes) {
        ByteBuf byteBuf = Unpooled.buffer();
        byteBuf.writeBytes(bytes);

        List<RaftNode> ret = new ArrayList<>();
        int count = byteBuf.readInt();

        for (int i = 0; i < count; i++) {
            ret.add(
                    RaftNode
                            .builder()
                            .groupId(MessageDecoderUtils.readString(byteBuf))
                            .id(MessageDecoderUtils.readString(byteBuf))
                            .ipAddress(MessageDecoderUtils.readString(byteBuf))
                            .port(byteBuf.readInt())
                            .build()
            );
        }

        byteBuf.release();

        return ret;
    }
}
