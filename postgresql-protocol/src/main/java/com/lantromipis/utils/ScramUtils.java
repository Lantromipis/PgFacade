package com.lantromipis.utils;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

public class ScramUtils {

    private static final byte[] INT_1 = new byte[]{0, 0, 0, 1};

    public static byte[] computeHmac(final byte[] keyBytes, String hmacName, final String string) throws InvalidKeyException, NoSuchAlgorithmException {
        SecretKeySpec key = new SecretKeySpec(keyBytes, hmacName);
        Mac mac = Mac.getInstance(hmacName);
        mac.init(key);

        mac.update(string.getBytes(StandardCharsets.US_ASCII));
        return mac.doFinal();
    }

    public static byte[] generateSaltedPassword(final String password,
                                                byte[] salt,
                                                int iterationsCount,
                                                String hmacName) throws InvalidKeyException, NoSuchAlgorithmException {


        Mac mac = createHmac(password.getBytes(StandardCharsets.US_ASCII), hmacName);

        mac.update(salt);
        mac.update(INT_1);
        byte[] result = mac.doFinal();

        byte[] previous = null;
        for (int i = 1; i < iterationsCount; i++) {
            mac.update(previous != null ? previous : result);
            previous = mac.doFinal();
            for (int x = 0; x < result.length; x++) {
                result[x] ^= previous[x];
            }
        }

        return result;
    }

    public static Mac createHmac(final byte[] keyBytes, String hmacName) throws NoSuchAlgorithmException,
            InvalidKeyException {

        SecretKeySpec key = new SecretKeySpec(keyBytes, hmacName);
        Mac mac = Mac.getInstance(hmacName);
        mac.init(key);
        return mac;
    }
}
