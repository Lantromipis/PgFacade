package com.lantromipis.constant;

import java.util.regex.Pattern;

public class PostgreSQLProtocolScramConstants {
    public static final String SASL_SHA_256_AUTH_MECHANISM_NAME = "SCRAM-SHA-256";
    public static final String SHA256_DIGEST_NAME = "SHA-256";
    public static final String SHA256_HMAC_NAME = "HmacSHA256";

    //for proxy auth
    public static final Pattern CLIENT_FIRST_MESSAGE_PATTERN = Pattern.compile("^(([pny])=?([^,]*),([^,]*),)(m?=?[^,]*,?n=([^,]*),r=([^,]*),?.*)$");
    public static final int CLIENT_FIRST_MESSAGE_GS2_HEADER_MATCHER_GROUP = 1;
    public static final int CLIENT_FIRST_MESSAGE_BARE_MATCHER_GROUP = 5;
    public static final int CLIENT_FIRST_MESSAGE_NONCE_MATCHER_GROUP = 7;

    public static final Pattern CLIENT_FINAL_MESSAGE_PATTERN = Pattern.compile("(c=([^,]*),r=([^,]*)),p=(.*)$");
    public static final int CLIENT_FINAL_MESSAGE_WITHOUT_PROOF_MATCHER_GROUP = 1;
    public static final int CLIENT_FINAL_MESSAGE_GS2_HEADER_MATCHER_GROUP = 2;
    public static final int CLIENT_FINAL_MESSAGE_NONCE_MATCHER_GROUP = 3;
    public static final int CLIENT_FINAL_MESSAGE_PROOF_MATCHER_GROUP = 4;
    public static final String SERVER_FIRST_MESSAGE_FORMAT = "r=%s,s=%s,i=%d";

    //for connection pool auth
    public static final String GS2_HEADER = "n,,";
    public static final String CLIENT_FIRST_MESSAGE_BARE_FORMAT = "n=%s,r=%s";
    public static final Pattern SERVER_FIRST_MESSAGE_PATTERN = Pattern.compile("r=([^,]*),s=([^,]*),i=(.*)$");
    public static final int SERVER_FIRST_MESSAGE_SERVER_NONCE_MATCHER_GROUP = 1;
    public static final int SERVER_FIRST_MESSAGE_SALT_MATCHER_GROUP = 2;
    public static final int SERVER_FIRST_MESSAGE_ITERATION_COUNT_MATCHER_GROUP = 3;
    public static final String CLIENT_FINAL_MESSAGE_FORMAT = "c=%s,r=%s,p=%s";
    public static final String CLIENT_FINAL_MESSAGE_WITHOUT_PROOF_FORMAT = "c=%s,r=%s";

    public static final Pattern SCRAM_SHA_256_PASSWD_FORMAT_PATTERN = Pattern.compile("SCRAM-SHA-256\\$(.*):(.*)\\$(.*):(.*)");
}

