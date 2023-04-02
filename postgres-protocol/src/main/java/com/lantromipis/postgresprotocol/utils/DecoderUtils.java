package com.lantromipis.postgresprotocol.utils;

import com.lantromipis.postgresprotocol.constant.PostgresProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.model.internal.MessageInfo;
import com.lantromipis.postgresprotocol.model.internal.SplitResult;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.ArrayList;
import java.util.List;

public class DecoderUtils {

    public static boolean checkIfMessageIsTermination(ByteBuf buf) {
        byte startChar = buf.readByte();
        buf.resetReaderIndex();

        return startChar == PostgresProtocolGeneralConstants.CLIENT_TERMINATION_MESSAGE_START_CHAR;
    }

    public static boolean containsMessageOfType(List<MessageInfo> messageInfos, byte targetMessageStartByte) {
        return messageInfos.stream()
                .anyMatch(messageInfo -> messageInfo.getStartByte() == targetMessageStartByte);
    }

    public static SplitResult splitToMessages(ByteBuf packet) {
        List<MessageInfo> messageInfos = new ArrayList<>();
        ByteBuf leftovers = null;

        if (packet.readableBytes() < 5) {
            leftovers = Unpooled.buffer();
            packet.readBytes(leftovers, packet.readableBytes());

            return SplitResult
                    .builder()
                    .messageInfos(messageInfos)
                    .lastIncompleteMessage(leftovers)
                    .build();
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
                leftovers = Unpooled.buffer(packet.readableBytes());
                leftovers.writeBytes(packet, packet.readableBytes());
                break;
            }

            ByteBuf message = Unpooled.buffer(entireMessageLength);
            message.writeBytes(packet, entireMessageLength);

            byte[] messageBytes = new byte[message.readableBytes()];
            message.readBytes(messageBytes, 0, messageBytes.length);

            message.release();

            messageInfos.add(MessageInfo
                    .builder()
                    .startByte(startByte)
                    .length(length)
                    .entireMessage(messageBytes)
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

        return SplitResult
                .builder()
                .messageInfos(messageInfos)
                .lastIncompleteMessage(leftovers)
                .build();
    }

    public static SplitResult splitToMessages(ByteBuf previousPacketLastIncompleteMessage, ByteBuf packet) {
        List<MessageInfo> messageInfos = new ArrayList<>();
        ByteBuf leftovers = null;

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
                ByteBuf buf = Unpooled.buffer();
                buf.writeBytes(previousPacketLastIncompleteMessage, prevAvailableMessageBytes);
                buf.writeBytes(packet, 5 - prevAvailableMessageBytes);

                previousPacketLastIncompleteMessage.resetReaderIndex();
                packet.resetReaderIndex();

                prevMsgStartByte = buf.readByte();
                prevMsgLength = buf.readInt();
            }

            int needToReadFromPacket = prevMsgLength - prevAvailableMessageBytes + 1;

            if (packet.readableBytes() >= needToReadFromPacket) {
                ByteBuf message = Unpooled.buffer(prevMsgLength + 1);

                message.writeBytes(previousPacketLastIncompleteMessage, prevAvailableMessageBytes);
                message.writeBytes(packet, needToReadFromPacket);

                byte[] messageBytes = new byte[message.readableBytes()];
                message.readBytes(messageBytes, 0, messageBytes.length);

                message.release();

                messageInfos.add(
                        MessageInfo
                                .builder()
                                .length(prevMsgLength)
                                .startByte(prevMsgStartByte)
                                .entireMessage(messageBytes)
                                .build()
                );

            } else {
                // new packet is incomplete too. All data to leftovers
                leftovers = Unpooled.buffer(prevAvailableMessageBytes + packet.readableBytes());
                leftovers.writeBytes(previousPacketLastIncompleteMessage, prevAvailableMessageBytes);
                leftovers.writeBytes(packet, packet.readableBytes());

                packet.readerIndex(0);

                return SplitResult
                        .builder()
                        .messageInfos(messageInfos)
                        .lastIncompleteMessage(leftovers)
                        .build();
            }
            previousPacketLastIncompleteMessage.release();
        }

        SplitResult splitResult = splitToMessages(packet);

        messageInfos.addAll(splitResult.getMessageInfos());

        return SplitResult
                .builder()
                .messageInfos(messageInfos)
                .lastIncompleteMessage(splitResult.getLastIncompleteMessage())
                .build();

    }

    private DecoderUtils() {
    }
}
