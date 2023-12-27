package com.lantromipis.postgresprotocol.utils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogSequenceNumberUtils {

    public static final long INVALID_LSN = 0;
    public static final Pattern WAL_PATTERN = Pattern.compile("^(.{8})(.{8})000000(.{2})$");

    public static boolean isWalFileName(String fileName) {
        Matcher matcher = WAL_PATTERN.matcher(fileName);
        return matcher.matches();
    }

    public static String extractTimelineFromWalFileName(String walFileName) {
        Matcher matcher = WAL_PATTERN.matcher(walFileName);
        matcher.matches();

        return matcher.group(1);
    }

    public static long getWalFileFirstLsn(String walFileName) {
        Matcher matcher = WAL_PATTERN.matcher(walFileName);
        matcher.matches();

        String lsnHigherPartStr = matcher.group(2);
        String lsnLowerPartStr = matcher.group(3);

        int lsnHigherPart = Integer.parseInt(lsnHigherPartStr, 16);
        int lsnLowerPart = Integer.parseInt(lsnLowerPartStr, 16);

        ByteBuf byteBuf = Unpooled.buffer(8);
        byteBuf.writeInt(lsnHigherPart);
        byteBuf.writeInt(lsnLowerPart);

        byteBuf.readerIndex(0);
        long value = byteBuf.readLong();
        byteBuf.release();

        return value;
    }

    public static String getWalFileFirstLsnAsString(String walFileName) {
        Matcher matcher = WAL_PATTERN.matcher(walFileName);
        matcher.matches();

        String lsnHigherPartStr = matcher.group(2);
        String lsnLowerPartStr = matcher.group(3);

        int lsnHigherPart = Integer.parseInt(lsnHigherPartStr, 16);
        int lsnLowerPart = Integer.parseInt(lsnLowerPartStr, 16);

        return String.format("%X/%X", lsnHigherPart, lsnLowerPart);
    }

    public static String lsnToString(long lsn) {
        ByteBuf buf = Unpooled.buffer(8);
        buf.writeLong(lsn);
        buf.readerIndex(0);

        int logicalXlog = buf.readInt();
        int segment = buf.readInt();

        return String.format("%X/%X", logicalXlog, segment);
    }

    public static long StringToLsn(String strValue) {
        int slashIndex = strValue.lastIndexOf('/');

        if (slashIndex <= 0) {
            return 0;
        }

        String logicalXLogStr = strValue.substring(0, slashIndex);
        int logicalXlog = (int) Long.parseLong(logicalXLogStr, 16);
        String segmentStr = strValue.substring(slashIndex + 1);
        int segment = (int) Long.parseLong(segmentStr, 16);

        ByteBuf byteBuf = Unpooled.buffer(8);
        byteBuf.writeInt(logicalXlog);
        byteBuf.writeInt(segment);

        byteBuf.readerIndex(0);
        long value = byteBuf.readLong();
        byteBuf.release();

        return value;
    }

    public static String getWalFileNameForLsn(long lsn, String timeline) {
        ByteBuf buf = Unpooled.buffer(8);
        buf.writeLong(lsn);

        int logicalXlog = buf.readInt();
        byte segmentHighestByte = buf.readByte();

        buf.release();

        return String.format("%s%08X000000%02X", timeline, logicalXlog, segmentHighestByte);
    }

    public static boolean compareIfBelongsToSameWal(long lsn1, long lsn2) {
        ByteBuf buf1 = Unpooled.buffer(8);
        buf1.writeLong(lsn1);

        ByteBuf buf2 = Unpooled.buffer(8);
        buf2.writeLong(lsn2);

        int logicalXlog1 = buf1.readInt();
        int logicalXlog2 = buf2.readInt();

        byte segmentHighestByte1 = buf1.readByte();
        byte segmentHighestByte2 = buf2.readByte();

        buf1.release();
        buf2.release();

        return logicalXlog1 == logicalXlog2 && segmentHighestByte1 == segmentHighestByte2;
    }

    public static long getFirstLsnInWalFileWithProvidedLsn(long lsn) {
        ByteBuf buf = Unpooled.buffer(8);
        buf.writeLong(lsn);

        int logicalXlog = buf.readInt();
        byte segmentHighestByte = buf.readByte();

        buf.release();

        ByteBuf retBuf = Unpooled.buffer(8);
        retBuf.writeInt(logicalXlog);
        retBuf.writeByte(segmentHighestByte);
        // 3 zero bytes
        retBuf.writeByte(0);
        retBuf.writeByte(0);
        retBuf.writeByte(0);

        retBuf.readerIndex(0);
        long value = retBuf.readLong();
        retBuf.release();

        return value;
    }

    public static int compareTwoLsn(long lsn1, long lsn2) {
        if (lsn1 == lsn2) {
            return 0;
        }
        //Unsigned comparison
        return lsn1 + Long.MIN_VALUE < lsn2 + Long.MIN_VALUE ? -1 : 1;
    }
}
