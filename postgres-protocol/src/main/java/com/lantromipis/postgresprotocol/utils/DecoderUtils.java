package com.lantromipis.postgresprotocol.utils;

import com.lantromipis.postgresprotocol.constant.PostgresProtocolGeneralConstants;
import com.lantromipis.postgresprotocol.model.internal.PgMessageInfo;
import com.lantromipis.postgresprotocol.model.protocol.DataRow;
import com.lantromipis.postgresprotocol.model.protocol.RowDescription;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

@Slf4j
public class DecoderUtils {

    public static void freeMessageInfos(Deque<PgMessageInfo> pgMessageInfos) {
        pgMessageInfos.forEach(m -> m.getEntireMessage().release());
        pgMessageInfos.clear();
    }

    public static boolean checkIfMessageIsTermination(ByteBuf buf) {
        byte startChar = buf.readByte();
        buf.resetReaderIndex();

        return startChar == PostgresProtocolGeneralConstants.CLIENT_TERMINATION_MESSAGE_START_CHAR;
    }

    public static boolean containsMessageOfType(Deque<PgMessageInfo> pgMessageInfos, byte targetMessageStartByte) {
        for (PgMessageInfo pgMessageInfo : pgMessageInfos) {
            if (pgMessageInfo.getStartByte() == targetMessageStartByte) {
                return true;
            }
        }

        return false;
    }

    public static boolean containsMessageOfTypeReversed(Deque<PgMessageInfo> pgMessageInfos, byte targetMessageStartByte) {
        Iterator<PgMessageInfo> iterator = pgMessageInfos.descendingIterator();

        while (iterator.hasNext()) {
            PgMessageInfo pgMessageInfo = iterator.next();
            if (pgMessageInfo.getStartByte() == targetMessageStartByte) {
                return true;
            }
        }

        return false;
    }

    public static String readNextNullTerminatedString(ByteBuf byteBuf) {
        byte[] byteArrayBuf = TempFastThreadLocalStorageUtils.getThreadLocalByteArray();
        int bytesIdx = 0;

        while (byteBuf.readerIndex() < byteBuf.writerIndex()) {
            byte b = byteBuf.readByte();

            if (b == PostgresProtocolGeneralConstants.DELIMITER_BYTE) {
                break;
            }

            byteArrayBuf[bytesIdx] = b;
            bytesIdx++;
        }

        if (bytesIdx != 0) {
            return new String(byteArrayBuf, 0, bytesIdx, StandardCharsets.UTF_8);
        }

        return null;
    }

    public static String readString(ByteBuf byteBuf, int length) {
        byte[] byteArrayBuf = TempFastThreadLocalStorageUtils.getThreadLocalByteArray();

        byteBuf.readBytes(byteArrayBuf, 0, length);

        return new String(byteArrayBuf, 0, length, StandardCharsets.UTF_8);
    }

    private static ByteBuf splitToMessages(ByteBuf packet, Deque<PgMessageInfo> retMessages, ByteBufAllocator allocator) {
        ByteBuf leftovers = null;

        // TODO move to while
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
                    PgMessageInfo
                            .builder()
                            .startByte(startByte)
                            .entireMessage(message)
                            .build()
            );

            // packet completed and all messages were split
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

    public static ByteBuf splitToMessages(ByteBuf previousPacketLastIncompleteMessage, ByteBuf packet, Deque<PgMessageInfo> retMessages, ByteBufAllocator allocator) {
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
                        PgMessageInfo
                                .builder()
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

    public static Map<RowDescription.FieldDescription, byte[]> mapDataRowColumnDescriptionByContent(RowDescription rowDescription, DataRow dataRow) {
        Map<RowDescription.FieldDescription, byte[]> ret = new HashMap<>();

        int size = dataRow.getColumns().size();
        for (int i = 0; i < size; i++) {
            byte[] column = dataRow.getColumns().get(i);
            RowDescription.FieldDescription columDescription = rowDescription.getFieldDescriptions().get(i);

            ret.put(columDescription, column);
        }

        return ret;
    }

    public static Map<String, String> mapDataRowColumnNameByContent(RowDescription rowDescription, DataRow dataRow) {
        Map<String, String> ret = new HashMap<>();

        int size = dataRow.getColumns().size();
        for (int i = 0; i < size; i++) {
            byte[] column = dataRow.getColumns().get(i);
            RowDescription.FieldDescription columDescription = rowDescription.getFieldDescriptions().get(i);

            if (column == null) {
                ret.put(columDescription.getFieldName(), null);
            } else {
                ret.put(columDescription.getFieldName(), new String(column, StandardCharsets.UTF_8));
            }
        }

        return ret;
    }

    private DecoderUtils() {
    }
}
