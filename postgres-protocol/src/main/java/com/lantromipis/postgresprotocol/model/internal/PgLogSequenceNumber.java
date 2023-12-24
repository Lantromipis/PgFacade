package com.lantromipis.postgresprotocol.model.internal;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;


/**
 * LSN (Log Sequence Number) data which is a pointer to a location in the XLOG.
 * COPIED FROM PgJDBC!!!
 */
public class PgLogSequenceNumber implements Comparable<PgLogSequenceNumber> {
    /**
     * Zero is used indicate an invalid pointer. Bootstrap skips the first possible WAL segment,
     * initializing the first WAL page at XLOG_SEG_SIZE, so no XLOG record can begin at zero.
     */
    public static final PgLogSequenceNumber INVALID_LSN = PgLogSequenceNumber.valueOf(0);

    private final long value;

    private PgLogSequenceNumber(long value) {
        this.value = value;
    }

    /**
     * @param value numeric represent position in the write-ahead log stream
     * @return not null LSN instance
     */
    public static PgLogSequenceNumber valueOf(long value) {
        return new PgLogSequenceNumber(value);
    }

    /**
     * Create LSN instance by string represent LSN.
     *
     * @param strValue not null string as two hexadecimal numbers of up to 8 digits each, separated by
     *                 a slash. For example {@code 16/3002D50}, {@code 0/15D68C50}
     * @return not null LSN instance where if specified string represent have not valid form {@link
     * PgLogSequenceNumber#INVALID_LSN}
     */
    public static PgLogSequenceNumber valueOf(String strValue) {
        int slashIndex = strValue.lastIndexOf('/');

        if (slashIndex <= 0) {
            return INVALID_LSN;
        }

        String logicalXLogStr = strValue.substring(0, slashIndex);
        int logicalXlog = (int) Long.parseLong(logicalXLogStr, 16);
        String segmentStr = strValue.substring(slashIndex + 1, strValue.length());
        int segment = (int) Long.parseLong(segmentStr, 16);

        ByteBuffer buf = ByteBuffer.allocate(8);
        buf.putInt(logicalXlog);
        buf.putInt(segment);
        buf.position(0);
        long value = buf.getLong();

        return PgLogSequenceNumber.valueOf(value);
    }

    /**
     * @return Long represent position in the write-ahead log stream
     */
    public long asLong() {
        return value;
    }

    /**
     * @return String represent position in the write-ahead log stream as two hexadecimal numbers of
     * up to 8 digits each, separated by a slash. For example {@code 16/3002D50}, {@code 0/15D68C50}
     */
    public String asString() {
        ByteBuf buf = Unpooled.buffer(8);
        buf.writeLong(value);
        buf.readerIndex(0);

        int logicalXlog = buf.readInt();
        int segment = buf.readInt();

        return String.format("%X/%X", logicalXlog, segment);
    }

    public String toWalName(String timeline) {
        ByteBuf buf = Unpooled.buffer(8);
        buf.writeLong(value);

        int logicalXlog = buf.readInt();
        byte segmentHighestByte = buf.readByte();

        buf.release();

        return String.format("%s%08X000000%02X", timeline, logicalXlog, segmentHighestByte);
    }

    public boolean compareIfBelongsToSameWal(PgLogSequenceNumber other) {
        if (other == null) {
            return false;
        }

        ByteBuf buf1 = Unpooled.buffer(8);
        buf1.writeLong(value);

        ByteBuf buf2 = Unpooled.buffer(8);
        buf2.writeLong(other.asLong());

        int logicalXlog1 = buf1.readInt();
        int logicalXlog2 = buf2.readInt();

        byte segmentHighestByte1 = buf1.readByte();
        byte segmentHighestByte2 = buf2.readByte();

        buf1.release();
        buf2.release();

        return logicalXlog1 == logicalXlog2 && segmentHighestByte1 == segmentHighestByte2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PgLogSequenceNumber that = (PgLogSequenceNumber) o;

        return value == that.value;

    }

    @Override
    public int hashCode() {
        return (int) (value ^ (value >>> 32));
    }

    @Override
    public String toString() {
        return "LSN{" + asString() + '}';
    }

    @Override
    public int compareTo(PgLogSequenceNumber o) {
        if (value == o.value) {
            return 0;
        }
        //Unsigned comparison
        return value + Long.MIN_VALUE < o.value + Long.MIN_VALUE ? -1 : 1;
    }
}
