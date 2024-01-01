package com.lantromipis.orchestration.constant;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;

public class ArchiverConstants {
    public static final Pattern WAL_FILE_NAME_PATTERN = Pattern.compile("[0-9a-fA-F]{24}");

    // S3
    public static final String S3_BACKUP_PREFIX = "backup";
    public static final String S3_WAL_PREFIX = "wal";
    public static final String S3_WAL_PREFIX_WITH_SLASH = S3_WAL_PREFIX + "/";
    public static final String S3_BACKUP_DATE_AND_WAL_SEPARATOR = "__";
    public static final String S3_BACKUP_KEY_FORMAT = S3_BACKUP_PREFIX + "/%s" + S3_BACKUP_DATE_AND_WAL_SEPARATOR + "%s.tar";
    public static final String S3_DUMMY_FIRST_WAL_FILE_NAME = "first_wal_name";
    public static final String S3_BACKUP_KEY_REGEX = S3_BACKUP_PREFIX + "\\/" + "(.*)" + S3_BACKUP_DATE_AND_WAL_SEPARATOR + "(.*)" + "\\.tar";
    public static final Pattern S3_BACKUP_KEY_PATTERN = Pattern.compile(S3_BACKUP_KEY_REGEX);
    public static final String S3_WAL_FILE_KEY_FORMAT = "wal/%s";
    public static final Pattern S3_WAL_FILE_KEY_PATTERN = Pattern.compile("wal\\/(.*)");

    // because ':' char is not safe to use in object key, so it will be replaced by '_'
    public static final DateTimeFormatter S3_BACKUP_KEY_DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH_mm_ss").withZone(ZoneId.from(ZoneOffset.UTC));

    private ArchiverConstants() {
    }
}
