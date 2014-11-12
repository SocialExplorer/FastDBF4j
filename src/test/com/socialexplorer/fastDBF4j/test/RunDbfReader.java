package com.socialexplorer.fastDBF4j.test;

import com.socialexplorer.fastDBF4j.DbfFile;
import com.socialexplorer.fastDBF4j.DbfHeader;
import com.socialexplorer.fastDBF4j.DbfRecord;

import java.io.FileNotFoundException;
import java.nio.charset.Charset;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: Maida
 * Date: 11/12/14
 * Time: 1:11 PM
 * To change this template use File | Settings | File Templates.
 */
public class RunDbfReader {
    public static void main(String[] args) throws Exception {
        //String dbfFilePath = "C:\\Users\\Maida\\Desktop\\tmp\\1220\\concatenated_dataset.dbf";
        String dbfFilePath = "C:\\Projects\\geoservices\\DataProcessorWorker\\src\\test\\resources\\merge_test\\StateH.dbf";

        String encoding = "UTF-8";

        //DbfFile dbfReader = new DbfFile(encoding);
        DbfFile dbfReader = new DbfFile();
        dbfReader.open(dbfFilePath, "r");

        DbfHeader dbfHeader = dbfReader.getHeader();
        DbfRecord dbfRecord = dbfReader.readNext();

        dbfRecord = dbfReader.readNext();
        String s = dbfRecord.get(0);
        s = dbfRecord.get(0);


        Map<String, Charset> cs = Charset.availableCharsets();
        Boolean b = Charset.isSupported("850");
    }
}
