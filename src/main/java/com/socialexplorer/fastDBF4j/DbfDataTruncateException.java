package com.socialexplorer.fastDBF4j;

import java.lang.Exception;

public class DbfDataTruncateException extends Exception {
    public DbfDataTruncateException(String message) {
        super(message);
    }

    public DbfDataTruncateException(String message, Exception innerException) {
        super(message, innerException);
    }

    /*public com.socialexplorer.geoservices.fastDbf.DbfDataTruncateException(SerializationInfo info, StreamingContext context):base(info, context) {
    } */

}
