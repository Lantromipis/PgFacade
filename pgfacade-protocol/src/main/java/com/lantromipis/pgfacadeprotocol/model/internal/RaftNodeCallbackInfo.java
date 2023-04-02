package com.lantromipis.pgfacadeprotocol.model.internal;

import com.lantromipis.pgfacadeprotocol.message.AbstractMessage;
import io.netty.channel.Channel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RaftNodeCallbackInfo {
    private AbstractMessage message;
    private Channel channel;
}
