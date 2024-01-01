package com.lantromipis.postgresprotocol.utils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogSequenceNumberUtils {

    public static final long INVALID_LSN = 0;
    public static final Pattern WAL_PATTERN = Pattern.compile("^(.{8})(.{8})000000(.{2})$");

    private static long xLogSegmentsPerXLogId(long walSegmentSize) {
        return 0x100000000L / walSegmentSize;
    }

    public static long extractTimelineFromWalFileName(String walFileName) {
        Matcher matcher = WAL_PATTERN.matcher(walFileName);
        matcher.matches();

        return Long.parseUnsignedLong(matcher.group(1), 16);
    }

    public static String timelineToStr(long timeline) {
        return String.format("%08X", timeline);
    }

    public static boolean isWalFileName(String fileName) {
        Matcher matcher = WAL_PATTERN.matcher(fileName);
        return matcher.matches();
    }

    public static long getWalFileFirstLsn(String walFileName, long walSegmentSize) {
        Matcher matcher = WAL_PATTERN.matcher(walFileName);
        matcher.matches();

        String xLogIdStr = matcher.group(2);
        String xLogSegmentNoStr = matcher.group(3);

        int xLogId = Integer.parseUnsignedInt(xLogIdStr, 16);
        int xLogSegmentNo = Integer.parseUnsignedInt(xLogSegmentNoStr, 16);

        return (xLogSegmentsPerXLogId(walSegmentSize) * xLogId + xLogSegmentNo) * walSegmentSize;
    }

    public static String getWalFileFirstLsnAsString(String walFileName, long walSegmentSize) {
        return lsnToString(getWalFileFirstLsn(walFileName, walSegmentSize));
    }

    public static String getWalFileNameForLsn(long timeline, long lsn, long walSegmentSize) {
        long logSegmentNo = Long.divideUnsigned(lsn, walSegmentSize);
        long xLogSegmentsPerXLogId = xLogSegmentsPerXLogId(walSegmentSize);

        return String.format(
                "%08X%08X%08X",
                timeline,
                logSegmentNo / xLogSegmentsPerXLogId,
                logSegmentNo % xLogSegmentsPerXLogId
        );
    }

    public static boolean compareIfBelongsToSameWal(long lsn1, long lsn2, long walSegmentSize) {
        long logSegmentNo1 = Long.divideUnsigned(lsn1, walSegmentSize);
        long logSegmentNo2 = Long.divideUnsigned(lsn2, walSegmentSize);

        return logSegmentNo1 == logSegmentNo2;
    }

    public static long getFirstLsnInWalFileWithProvidedLsn(long lsn, long walSegmentSize) {
        long logSegmentNo = Long.divideUnsigned(lsn, walSegmentSize);
        return logSegmentNo * walSegmentSize;
    }

    public static long getNextWalFileStartLsn(String currentWalFileName, long walSegmentSize) {
        long currentFileFirstLsn = getWalFileFirstLsn(currentWalFileName, walSegmentSize);
        long currentFileFirstLsnLogSegNo = Long.divideUnsigned(currentFileFirstLsn, walSegmentSize);
        return (currentFileFirstLsnLogSegNo + 1) * walSegmentSize;
    }

    public static String lsnToString(long lsn) {
        ByteBuf buf = Unpooled.buffer(8);
        buf.writeLong(lsn);
        buf.readerIndex(0);

        int logicalXlog = buf.readInt();
        int segment = buf.readInt();

        return String.format("%X/%X", logicalXlog, segment);
    }

    public static long stringToLsn(String strValue) {
        int slashIndex = strValue.lastIndexOf('/');

        if (slashIndex <= 0) {
            return 0;
        }

        String logicalXLogStr = strValue.substring(0, slashIndex);
        int logicalXlog = Integer.parseUnsignedInt(logicalXLogStr, 16);
        String segmentStr = strValue.substring(slashIndex + 1);
        int segment = Integer.parseUnsignedInt(segmentStr, 16);

        ByteBuf byteBuf = Unpooled.buffer(8);
        byteBuf.writeInt(logicalXlog);
        byteBuf.writeInt(segment);

        byteBuf.readerIndex(0);
        long value = byteBuf.readLong();
        byteBuf.release();

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
