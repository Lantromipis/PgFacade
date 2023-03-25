package com.lantromipis.pgfacadeprotocol.model.api;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Objects;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RaftNode {
    private String groupId;
    private String id;
    private String ipAddress;
    private int port;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RaftNode raftNode = (RaftNode) o;

        if (!Objects.equals(groupId, raftNode.groupId)) return false;
        return Objects.equals(id, raftNode.id);
    }

    @Override
    public int hashCode() {
        int result = groupId != null ? groupId.hashCode() : 0;
        result = 31 * result + (id != null ? id.hashCode() : 0);
        return result;
    }
}
