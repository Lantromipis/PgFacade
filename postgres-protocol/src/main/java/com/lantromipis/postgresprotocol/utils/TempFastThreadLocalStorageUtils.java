package com.lantromipis.postgresprotocol.utils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.FastThreadLocal;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class TempFastThreadLocalStorageUtils {

    private static final int BYTE_TEMP_ARRAY_SIZE = 65535;
    private static final int BYTE_BUF_SIZE = 65535;

    private static final FastThreadLocal<byte[]> BYTE_ARRAY = new FastThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() throws Exception {
            return new byte[BYTE_TEMP_ARRAY_SIZE];
        }
    };

    private static final FastThreadLocal<ByteBuf> BYTE_BUF = new FastThreadLocal<ByteBuf>() {

        private AtomicBoolean removed;
        private ByteBuf byteBuf;

        @Override
        protected ByteBuf initialValue() throws Exception {
            removed = new AtomicBoolean(false);
            byteBuf = Unpooled.directBuffer(BYTE_BUF_SIZE);
            return byteBuf;
        }

        @Override
        protected void onRemoval(ByteBuf value) throws Exception {
            super.onRemoval(value);

            if (removed.compareAndSet(false, true)) {
                byteBuf.release();
            }
        }
    };

    public static byte[] getThreadLocalByteArray() {
        return BYTE_ARRAY.get();
    }

    public static ByteBuf getThreadLocalByteBuf() {
        return BYTE_BUF.get();
    }
}
