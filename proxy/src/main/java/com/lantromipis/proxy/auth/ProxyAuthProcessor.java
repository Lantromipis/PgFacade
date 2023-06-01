package com.lantromipis.proxy.auth;

import com.lantromipis.connectionpool.model.auth.PoolAuthInfo;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

public interface ProxyAuthProcessor {
    /**
     * Process auth of client. In case of failed auth must close channel.
     *
     * @param ctx     ChannelHandlerContext
     * @param message ByteBuf
     * @return object containing info for pool to authenticate user or null if auth not completed
     * @throws Exception if something went wrong
     */
    PoolAuthInfo processAuth(ChannelHandlerContext ctx, ByteBuf message) throws Exception;
}
