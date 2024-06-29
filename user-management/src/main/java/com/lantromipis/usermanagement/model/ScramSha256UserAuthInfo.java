package com.lantromipis.usermanagement.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@SuperBuilder
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class ScramSha256UserAuthInfo extends UserAuthInfo {
    private int iterationCount;
    private String salt;
    private String storedKey;
    private String serverKey;

    private byte[] storedKeyDecodedBytes;
    private byte[] serverKeyDecodedBytes;
}
