package com.lantromipis.orchestration.constant;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;

public class ArchiverConstants {

    // S3
    public static final String S3_BACKUP_PREFIX = "backup";
    public static final String S3_WAL_PREFIX = "wal";
    public static final String S3_BACKUP_KEY_FORMAT = "backup/%s.tar";
    public static final Pattern S3_BACKUP_KEY_PATTERN = Pattern.compile("backup\\/(.*)\\.tar");

    // because ':' char is not safe to use in object key, so it will be replaced by '_'
    public static final DateTimeFormatter S3_BACKUP_KEY_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH_mm_ss").withZone(ZoneId.from(ZoneOffset.UTC));

    private ArchiverConstants() {
    }
}
