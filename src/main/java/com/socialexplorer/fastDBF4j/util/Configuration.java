package com.socialexplorer.fastDBF4j.util;

public class Configuration {
    /**
     * Encoding to be used if it is not provided by the user and the encoding written in DBF language driver
     * is not valid.
     */
    public static final String DEFAULT_ENCODING_NAME = "windows-1252";
    /**
     * Encoding name that is used.
     */
    private String encodingName;
    /**
     * True if encoding is provided by the user (should be forced). False if it should be read from dbf.
     */
    private Boolean shouldTryToSetEncodingFromLanguageDriver;

    public String getEncodingName() {
        return encodingName;
    }

    public void setEncodingName(String encodingName) {
        this.encodingName = encodingName;
    }

    public Boolean getShouldTryToSetEncodingFromLanguageDriver() {
        return shouldTryToSetEncodingFromLanguageDriver;
    }

    public void setShouldTryToSetEncodingFromLanguageDriver(Boolean shouldTryToSetEncodingFromLanguageDriver) {
        this.shouldTryToSetEncodingFromLanguageDriver = shouldTryToSetEncodingFromLanguageDriver;
    }
}
