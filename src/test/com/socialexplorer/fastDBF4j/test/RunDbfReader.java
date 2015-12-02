package com.socialexplorer.fastDBF4j.test;

import com.socialexplorer.fastDBF4j.DbfColumn;
import com.socialexplorer.fastDBF4j.DbfFile;
import com.socialexplorer.fastDBF4j.DbfRecord;
import com.socialexplorer.fastDBF4j.exceptions.DbfDataTruncateException;
import com.socialexplorer.fastDBF4j.exceptions.InvalidDbfFileException;

import java.io.IOException;

public class RunDbfReader {
    public static void main(String[] args) throws Exception {
        readRecord();
    }

    private static void addColumn() throws IOException, InvalidDbfFileException, DbfDataTruncateException {
        String path = "/home/maida/tmp/datahub-java-workers/test_data/new.dbf";
        DbfFile dbfFile = new DbfFile(path, "rw", true);
        dbfFile.open();

        DbfColumn dbfColumn = new DbfColumn("kolona", DbfColumn.DbfColumnType.CHARACTER, 10, 0);
        dbfFile.getHeader().addColumn(dbfColumn);

        DbfRecord dbfRecord = new DbfRecord(dbfFile.getHeader());
        dbfRecord.set(0, "aaa");
        dbfFile.write(dbfRecord);
    }


    private static void readRecord() throws IOException, InvalidDbfFileException {
        String path = "...";
        DbfFile dbfReader = new DbfFile(path, "r", true);
        dbfReader.open();

        DbfRecord dbfRecord;
        while ((dbfRecord = dbfReader.readNext()) != null) {
            dbfRecord.get(0);
        }
    }

}
