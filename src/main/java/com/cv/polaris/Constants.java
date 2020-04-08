package com.cv.polaris;

public final class Constants {

    public static final int NUM_PARTITIONS = 36;
    public static final int DF_FROM_ORACLE = 0;
    public static final int MOD_NUM = 128;
    public static final String ENVIRONMENT = "ENVIRONMENT";
    public static final String SHA_ALGORITHM = "SHA-1";
    public static final String GET_MOD_VALUE = "getModValue";
    public static final String GET_EPOCH_TIMEZONE = "toEpochMSTimeZone";
    public static final String ODS_STORE = "hive";
    public static final String ODS_LKP_STORE = "hbase";
    public static final String COLON = ":";
    public static final String PST = "America/LosAngeles";
    public static final String IGNORE_USER_DEFINED_EXCEPTION_SET = "Y";
    public static final String IS_GROUPING_REQUIRED = "IS_GROUPING_REQUIRED";
    public static final String DRIVER = "oracle.jdbc.OracleDriver";
    public static final String CONFIG_PROPERTIES = "config.properties";
    public static final String KAFKA_KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    public static final String KAFKA_VALUE_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer";

    public static final int THOUSAND = 1000;

    public static final String MD5_DIGEST = "MD5";
    public static final String HYPHEN = "-";
    public static final Integer ONE = 1;
    public static final Integer SIXTEEN = 16;
    public static final Integer THIRTY_TWO = 32;
    public static final String STRING_ZERO_INTEGER = "0";
    public static final String INSERT_INTO_NRT_PAYMENT_EVENT_DELTA = "insert into some table";
    //JDBCSink Query
    public static final String MERGE_INTO_FINAL_TABLE = "merge into some table";

    public static final String INSERT_NRT_TEST6 = "INSERT INTO another table";

    private Constants() {
        throw new IllegalStateException("You are not suppose to do this.");
    }
}