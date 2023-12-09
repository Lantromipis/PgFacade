package com.lantromipis.proxy.service.api;

import io.netty.buffer.ByteBuf;

import java.nio.channels.Channel;

public interface PgMessageProcessingService {

    void processMessageForSingleMasterLoadBalancingCase(ByteBuf message, Channel primaryChannel, Channel standbyChannel);
}
