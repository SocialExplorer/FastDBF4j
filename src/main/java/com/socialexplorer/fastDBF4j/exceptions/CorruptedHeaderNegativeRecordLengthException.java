package com.socialexplorer.fastDBF4j.exceptions;

public class CorruptedHeaderNegativeRecordLengthException extends InvalidDbfFileException {
    public CorruptedHeaderNegativeRecordLengthException(Integer invalidRecordLength) {
        super("Expected non-negative record length definition in header but got " + invalidRecordLength);
    }
}
