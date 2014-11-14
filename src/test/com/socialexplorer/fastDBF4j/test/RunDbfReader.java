package com.socialexplorer.fastDBF4j.test;

import com.socialexplorer.fastDBF4j.DbfFile;
import com.socialexplorer.fastDBF4j.DbfHeader;
import com.socialexplorer.fastDBF4j.DbfRecord;

import java.nio.charset.Charset;
import java.util.Map;

public class RunDbfReader {
    public static void main(String[] args) throws Exception {
        //String dbfFilePath = "C:\\Users\\Maida\\Desktop\\tmp\\1220\\concatenated_dataset.dbf";
        String dbfFilePath = "C:\\Projects\\geoservices\\DataProcessorWorker\\src\\test\\resources\\merge_test\\StateH.dbf";

        String encoding = "UTF-8";

        //DbfFile dbfReader = new DbfFile(encoding);
        DbfFile dbfReader = new DbfFile(dbfFilePath, "r");
        dbfReader.open();

        DbfHeader dbfHeader = dbfReader.getHeader();
        DbfRecord dbfRecord = dbfReader.readNext();

        dbfRecord = dbfReader.readNext();
        String s = dbfRecord.get(0);
        s = dbfRecord.get(0);


        Map<String, Charset> cs = Charset.availableCharsets();
        Boolean b = Charset.isSupported("850");
    }
}
