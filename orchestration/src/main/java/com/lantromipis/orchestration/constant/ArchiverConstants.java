package com.lantromipis.orchestration.constant;

import java.math.BigInteger;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;

public class ArchiverConstants {

    public static final int WAL_FILE_NAME_LENGTH = 24;

    public static final String PARTIAL_WAL_FILE_ENDING = ".partial";
    public static final String TMP_HISTORY_FILE_ENDING = ".tmp";
    public static final String HISTORY_FILE_ENDING = ".history";
    public static final BigInteger PARTIAL_WAL_FILE_REMOVE_DIFF = BigInteger.valueOf(4L);

    // S3
    public static final String S3_BACKUP_PREFIX = "backup";
    public static final String S3_WAL_PREFIX = "wal";
    public static final String S3_BACKUP_KEY_FORMAT = "backup/%s.tar";
    public static final Pattern S3_BACKUP_KEY_PATTERN = Pattern.compile("backup\\/(.*)\\.tar");
    public static final String S3_WAL_FILE_KEY_FORMAT = "wal/%s";
    public static final Pattern S3_WAL_FILE_KEY_PATTERN = Pattern.compile("wal\\/(.*)");
    public static final String S3_BACKUP_FIRST_REQUIRED_WAL_METADATA_KEY = "first-required-wal-file";

    // because ':' char is not safe to use in object key, so it will be replaced by '_'
    public static final DateTimeFormatter S3_BACKUP_KEY_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH_mm_ss").withZone(ZoneId.from(ZoneOffset.UTC));

    private ArchiverConstants() {
    }
}
