package com.lantromipis.postgresprotocol.utils;

import com.lantromipis.postgresprotocol.constant.PostgresProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.model.internal.MessageInfo;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.util.List;

public class DecoderUtils {

    public static void freeMessageInfos(List<MessageInfo> messageInfos) {
        messageInfos.forEach(m -> m.getEntireMessage().release());
        messageInfos.clear();
    }

    public static boolean checkIfMessageIsTermination(ByteBuf buf) {
        byte startChar = buf.readByte();
        buf.resetReaderIndex();

        return startChar == PostgresProtocolGeneralConstants.CLIENT_TERMINATION_MESSAGE_START_CHAR;
    }

    public static boolean containsMessageOfType(List<MessageInfo> messageInfos, byte targetMessageStartByte) {
        for (MessageInfo messageInfo : messageInfos) {
            if (messageInfo.getStartByte() == targetMessageStartByte) {
                return true;
            }
        }

        return false;
    }

    public static boolean containsMessageOfTypeReversed(List<MessageInfo> messageInfos, byte targetMessageStartByte) {
        for (int i = messageInfos.size() - 1; i >= 0; i--) {
            MessageInfo messageInfo = messageInfos.get(i);
            if (messageInfo.getStartByte() == targetMessageStartByte) {
                return true;
            }
        }

        return false;
    }

    private static ByteBuf splitToMessages(ByteBuf packet, List<MessageInfo> retMessages, ByteBufAllocator allocator) {
        ByteBuf leftovers = null;

        if (packet.readableBytes() < 5) {
            leftovers = allocator.buffer(packet.readableBytes());
            packet.readBytes(leftovers, packet.readableBytes());

            return leftovers;
        }

        while (true) {
            // mark message beginning
            packet.markReaderIndex();
            byte startByte = packet.readByte();

            if (startByte == 0) {
                break;
            }

            int length = packet.readInt();

            if (length == 0) {
                break;
            }

            // reset to message beginning
            packet.resetReaderIndex();

            int entireMessageLength = length + 1;

            if (packet.readableBytes() < entireMessageLength) {
                leftovers = allocator.buffer(packet.readableBytes());
                leftovers.writeBytes(packet, packet.readableBytes());
                break;
            }

            ByteBuf message = allocator.buffer(entireMessageLength);
            message.writeBytes(packet, entireMessageLength);

            retMessages.add(
                    MessageInfo
                            .builder()
                            .startByte(startByte)
                            .length(length)
                            .entireMessage(message)
                            .build()
            );

            // packet completed and all messaged were split
            if (packet.readableBytes() == 0) {
                break;
            }

            // packet still got data, but it is not enough to get message info
            if (packet.readableBytes() <= 5) {
                leftovers = packet.readBytes(packet.readableBytes());
                break;
            }
        }

        packet.readerIndex(0);

        return leftovers;
    }

    public static ByteBuf splitToMessages(ByteBuf previousPacketLastIncompleteMessage, ByteBuf packet, List<MessageInfo> retMessages, ByteBufAllocator allocator) {
        ByteBuf leftovers;

        // for previous incomplete message
        if (previousPacketLastIncompleteMessage != null && previousPacketLastIncompleteMessage.readableBytes() > 0) {
            byte prevMsgStartByte;
            int prevMsgLength;

            int prevAvailableMessageBytes = previousPacketLastIncompleteMessage.readableBytes();

            // mark message beginning
            previousPacketLastIncompleteMessage.markReaderIndex();
            packet.markReaderIndex();

            // enough data to get message info
            if (prevAvailableMessageBytes >= 5) {
                prevMsgStartByte = previousPacketLastIncompleteMessage.readByte();
                prevMsgLength = previousPacketLastIncompleteMessage.readInt();

                previousPacketLastIncompleteMessage.resetReaderIndex();
            } else {
                ByteBuf buf = allocator.buffer(5);
                buf.writeBytes(previousPacketLastIncompleteMessage, prevAvailableMessageBytes);
                buf.writeBytes(packet, 5 - prevAvailableMessageBytes);

                previousPacketLastIncompleteMessage.resetReaderIndex();
                packet.resetReaderIndex();

                prevMsgStartByte = buf.readByte();
                prevMsgLength = buf.readInt();

                buf.release();
            }

            // 1 byte for message type
            int needToReadFromPacket = prevMsgLength - prevAvailableMessageBytes + 1;

            if (packet.readableBytes() >= needToReadFromPacket) {
                ByteBuf message = allocator.buffer(prevMsgLength + 1);

                message.writeBytes(previousPacketLastIncompleteMessage, prevAvailableMessageBytes);
                message.writeBytes(packet, needToReadFromPacket);

                retMessages.add(
                        MessageInfo
                                .builder()
                                .length(prevMsgLength)
                                .startByte(prevMsgStartByte)
                                .entireMessage(message)
                                .build()
                );

            } else {
                // new packet is incomplete too. All data to leftovers
                leftovers = allocator.buffer(prevAvailableMessageBytes + packet.readableBytes());
                leftovers.writeBytes(previousPacketLastIncompleteMessage, prevAvailableMessageBytes);
                leftovers.writeBytes(packet, packet.readableBytes());

                packet.readerIndex(0);

                return leftovers;
            }
        }

        leftovers = splitToMessages(packet, retMessages, allocator);

        return leftovers;

    }

    private DecoderUtils() {
    }
}
