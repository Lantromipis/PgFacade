package com.lantromipis.proxy.service.impl;

import com.lantromipis.proxy.service.api.PgMessageProcessingService;
import io.netty.buffer.ByteBuf;
import jakarta.enterprise.context.ApplicationScoped;

import java.nio.channels.Channel;

@ApplicationScoped
public class PgMessageProcessingServiceImpl implements PgMessageProcessingService {
    @Override
    public void processMessageForSingleMasterLoadBalancingCase(ByteBuf message, Channel primaryChannel, Channel standbyChannel) {

    }
}
