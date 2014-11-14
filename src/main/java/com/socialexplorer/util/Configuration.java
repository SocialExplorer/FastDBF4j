package com.socialexplorer.util;

public class Configuration {
    /**
     * Encoding to be used if it is not provided by the user and the encoding written in DBF language driver
     * is not valid.
     */
    public static final String DEFAULT_ENCODING_NAME = "windows-1252";
    /**
     * Encoding name that is used.
     */
    private static String encodingName;
    /**
     * True if encoding is provided by the user (should be forced). False if it should be read from dbf.
     */
    private static Boolean shouldTryToSetEncodingFromLanguageDriver;

    public static String getEncodingName() {
        return encodingName;
    }

    public static void setEncodingName(String encodingName) {
        Configuration.encodingName = encodingName;
    }

    public static Boolean getShouldTryToSetEncodingFromLanguageDriver() {
        return shouldTryToSetEncodingFromLanguageDriver;
    }

    public static void setShouldTryToSetEncodingFromLanguageDriver(Boolean shouldTryToSetEncodingFromLanguageDriver) {
        Configuration.shouldTryToSetEncodingFromLanguageDriver = shouldTryToSetEncodingFromLanguageDriver;
    }
}
