package com.lantromipis.postgresprotocol.utils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.concurrent.FastThreadLocal;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.microprofile.config.ConfigProvider;

import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class TempFastThreadLocalStorageUtils {

    private static final String TEMP_BYTE_ARRAY_SIZE_SETTING_NAME = "pg-facade.buffers.thread-local-byte-array-size";
    private static final String TEMP_BYTE_BUF_SIZE_SETTING_NAME = "pg-facade.buffers.thread-local-byte-buf-size";

    private static final int TEMP_BYTE_ARRAY_SIZE = ConfigProvider.getConfig()
            .getOptionalValue(TEMP_BYTE_ARRAY_SIZE_SETTING_NAME, Integer.class)
            .orElse(65537);
    private static final int BYTE_BUF_SIZE = ConfigProvider.getConfig()
            .getOptionalValue(TEMP_BYTE_BUF_SIZE_SETTING_NAME, Integer.class)
            .orElse(65537);

    private static final FastThreadLocal<byte[]> BYTE_ARRAY = new FastThreadLocal<byte[]>() {
        @Override
        protected byte[] initialValue() throws Exception {
            return new byte[TEMP_BYTE_ARRAY_SIZE];
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

    public static byte[] getThreadLocalByteArray(int expectedSize) {
        if (expectedSize <= TEMP_BYTE_ARRAY_SIZE) {
            return BYTE_ARRAY.get();
        } else {
            log.warn(
                    "Requested temp byte array buf with size {} when configured size is {}. " +
                            "Allocating new byte array! " +
                            "Consider increasing setting {} to improve performance and memory consumption! ",
                    expectedSize,
                    TEMP_BYTE_ARRAY_SIZE,
                    TEMP_BYTE_ARRAY_SIZE_SETTING_NAME
            );
            return new byte[expectedSize];
        }
    }

    public static ByteBuf getThreadLocalByteBuf() {
        return BYTE_BUF.get();
    }
}
