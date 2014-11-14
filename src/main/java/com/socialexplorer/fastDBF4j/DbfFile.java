package com.socialexplorer.fastDBF4j;

import com.socialexplorer.fastDBF4j.exceptions.InvalidDbfFileException;
import com.socialexplorer.util.Configuration;
import com.socialexplorer.util.FileReader;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

/**
 * This class represents a DBF file. You can create new, open, update and save DBF files using this class and supporting classes.
 * Also, this class supports reading/writing from/to an internet forward only type of stream!
 * TODO: add end of file byte '0x1A' !!!
 * We don't rely on that byte at all, and everything works with or without that byte, but it should be there by spec.
 *
 * TODO
 * Forbid file editing if the encoding is UTF-8 since column width is variable in that case.
 * It would be a pain in the ass to modify the header to fix column size. Also all values after
 * that column would have to be shifted in case a more-than-one byte character appears..

 */
public class DbfFile {
    /**
     * Header object that helps read/write dbf file header information.
     */
    protected DbfHeader header = new DbfHeader();
    /**
     * Flag that indicates whether the header was written or not.
     */
    protected boolean headerWritten = false;
    /**
     * File reader/writer.
     */
    protected FileReader dbfFile = null;
    /**
     * Path to the file, if the file was opened at all.
     */
    protected String filePath = "";
    /**
     * Number of records read using readNext() methods only. This applies only when we are using a forward-only stream.
     * recordsReadCount is used to keep track of record index. With a seek enabled stream,
     * we can always calculate index using stream position.
     */
    protected int recordsReadCount = 0;
    /**
     * The file can be read in forward direction only.
     * Keep isForwardOnly handy so we don't call functions on every read.
     */
    protected boolean isForwardOnly = false;
    /**
     * The file cannot be written to.
     * Keep isReadOnly handy so we don't call functions on every read.
     */
    protected boolean isReadOnly = false;
    /**
     * "r" - Read-only, "rw" - Read/write
     */
    protected String fileAccess = "";

    /**
     * Initialize DBF file.
     * @param filePath Path to the file.
     * @param fileAccess "r" for read-only or "rw" for read/write.
     * @param encodingName Encoding to be used for reading the file.
     * @param shouldTryToSetEncodingFromLanguageDriver True if the encoding should be read from the DBF's language
     *                                                 driver that is written in the header, false otherwise.
     *                                                 If true and the encoding cannot be read from the DBF's header,
     *                                                 windows-1252 is used as default.
     * @exception IllegalArgumentException If file access is neither 'r' nor 'rw'
     * @exception UnsupportedCharsetException If the encoding provided in the cpg file is not valid or not supported.
     */
    private void initialize(String filePath, String fileAccess, String encodingName, boolean shouldTryToSetEncodingFromLanguageDriver) {
        this.filePath = filePath;

        if (!(fileAccess.equals("r") || fileAccess.equals("rw"))) {
            throw new IllegalArgumentException("File access must be either 'r' or 'rw'");
        }
        this.fileAccess = fileAccess;

        Configuration.setShouldTryToSetEncodingFromLanguageDriver(shouldTryToSetEncodingFromLanguageDriver);

        if (!Charset.isSupported(encodingName)) {
            throw new UnsupportedCharsetException("Encoding :" + encodingName + " not supported!");
        }
        Configuration.setEncodingName(encodingName);
    }

    /**
     * Creates a DBF file object. Since there is no cpg file provided in this constructor, encoding will be read from
     * the dbf language driver ID. If there is no data in the language driver ID, windows-1252 will be used as default.
     *
     * @param filePath Full path to the file.
     * @param fileAccess read - "r", read/write - "rw"
     */
    public DbfFile(String filePath, String fileAccess) {
        initialize(filePath, fileAccess, Configuration.DEFAULT_ENCODING_NAME, true);
    }

    /**
     * @param filePath Full path to the file.
     * @param fileAccess read - "r", read/write - "rw"
     * @param cpgFilePath Path to the CPG file that contains encoding information on the first line.
     * @exception IllegalArgumentException If the CPG file is empty.
     * @throws IOException If the cpg file cannot be read.
     */
    public DbfFile(String filePath, String fileAccess, String cpgFilePath) throws IOException {
        // Read cpg file
        List<String> lines = Files.readAllLines(Paths.get(cpgFilePath), StandardCharsets.UTF_8);
        if (lines.size() == 0) {
            throw new IllegalArgumentException("CPG file is empty. File path: " + cpgFilePath);
        }
        // Encoding should be on the first line.
        String encodingName = lines.get(0);

        initialize(filePath, fileAccess, encodingName, false);
    }

    /**
     * Open a DBF file or create a new one.
     * @throws FileNotFoundException true if the file path is invalid.
     * @throws InvalidDbfFileException If the DBF file is not valid.
     */
    public void open() throws IOException, InvalidDbfFileException {
        recordsReadCount = 0; // reset position
        headerWritten = false; // assume the header is not written
        isForwardOnly = false; // RandomAccessFile can seek TODO check if this is needed
        isReadOnly = (fileAccess.equals("r"));

        dbfFile = new com.socialexplorer.util.FileReader(new RandomAccessFile(filePath, fileAccess)); // TODO move package

        // read the header
        try {
            header.read(dbfFile);
            headerWritten = true;
        } catch (EOFException e) {
            // could not read the header because file is empty
            header = new DbfHeader();
            headerWritten = false;
        }
    }

    /**
     * Update header info, flush buffers and close streams. This method should always be called
     * when done working with a DBF file.
     * @throws IOException  if an I/O error occurs.
     */
    public void close() throws IOException {
        // Try to update the header if it has changed.
        if (header.getIsDirty()) {
            writeHeader();
        }

        // Create an empty header.
        header = new DbfHeader();
        headerWritten = false;

        // Reset current record index.
        recordsReadCount = 0;

        // Close file.
        if (dbfFile != null) {
            dbfFile.close();
        }
        dbfFile = null;

        filePath = "";
    }

    /**
     * @return true if DBF file cannot be written to, false otherwise.
     */
    public boolean isReadOnly() {
        return isReadOnly;
    }

    /**
     * @return true if seeking is not possible, false otherwise TODO check if necessary
     */
    public boolean isForwardOnly() {
        return isForwardOnly;
    }

    /**
     * @return The name of the file path.
     */
    public String getFilePath() {
        return filePath;
    }

    /**
     * Read next record and fill data into parameter _fillRecord.
     * @param fillRecord DbfRecord object into which the record is stored
     * @return true if a record was read, false otherwise
     * @exception IllegalStateException If the file reader/writer is null.
     * @exception IllegalStateException If the size of the record does not match the size written in header.
     * @throws IOError If an I/O error occurs.
     */
    public boolean readNext(DbfRecord fillRecord) throws IOException {
        // check if we can fill this record with data. it must match record size specified by header and number of columns.
        // we are not checking whether it comes from another DBF file or not, we just need the same structure. Allow flexibility but be safe.
        if (fillRecord.getHeader() != header && (fillRecord.getHeader().getColumnCount() != header.getColumnCount() || fillRecord.getHeader().getRecordLength() != header.getRecordLength()))
            throw new IllegalStateException("Record parameter does not have the same size and number of columns as the " +
                                "header specifies, so we are unable to read a record into fillRecord. " +
                                "This is a programming error, have you mixed up DBF file objects?");

        // DBF file reader can be null if stream is not readable.
        if (dbfFile == null) {
            throw new IllegalStateException("Read stream is null, either you have opened a stream that can not be " +
                                "read from (a write-only stream) or you have not opened a stream at all.");
        }

        // read next record...
        boolean readSuccess = fillRecord.read(dbfFile);

        if (readSuccess) {
            if (isForwardOnly) {
                // zero based index! set before incrementing count.
                fillRecord.setRecordIndex(recordsReadCount);
                recordsReadCount++;
            } else {
                fillRecord.setRecordIndex(((int) ((dbfFile.getFilePointer() - header.HeaderLength()) / header.getRecordLength())) - 1);
            }
        }

        return readSuccess;
    }

    /**
     * Reads the next record.
     *
     * @return a new record object or null if nothing was read
     * @throws UnsupportedEncodingException If the encoding is not valid or is not supported.
     * @throws IOException If an I/O error occurs.
     */
    public DbfRecord readNext() throws IOException {
        // create a new record and fill it
        DbfRecord nextRecord = new DbfRecord(header);

        if (readNext(nextRecord)) {
            return nextRecord;
        } else {
            return null;
        }
    }

    /**
     * Reads a record specified by index into fillRecord object. You can use this method
     * to read in and process records without creating and discarding record objects.
     * Note that you should check that your stream is not forward-only! If you have a forward only stream, use readNext() functions.
     *
     * @param index      Zero based record index.
     * @param fillRecord Record object to fill, must have same size and number of fields as thid DBF file header!
     *                   The parameter record (fillRecord) must match record size specified by the header and number of columns as well.
     *                   It does not have to come from the same header, but it must match the structure. We are not going as far as to check size of each field.
     *                   The idea is to be flexible but safe. It's a fine balance, these two are almost always at odds.
     * @return True if read a record was read, otherwise false. If you read end of file false will be returned and fillRecord will NOT be modified!
     * @throws IOException If an I/O error occurs.
     * @exception IllegalStateException If the DBF reader/writer is null.
     * @exception IllegalStateException If the size of the record does not match the size written in header.
     */
    public boolean read(int index, DbfRecord fillRecord) throws IOException {
        // check if we can fill this record with data. it must match record size specified by header and number of columns.
        // we are not checking whether it comes from another DBF file or not, we just need the same structure. Allow flexibility but be safe.
        if (fillRecord.getHeader() != header && (fillRecord.getHeader().getColumnCount() != header.getColumnCount() || fillRecord.getHeader().getRecordLength() != header.getRecordLength()))
            throw new IllegalStateException("Record parameter does not have the same size and number of columns as the " +
                                "header specifies, so we are unable to read a record into oFillRecord. " +
                                "This is a programming error, have you mixed up DBF file objects?");

        // DBF file reader can be null if stream is not readable...
        if (dbfFile == null)
            throw new IllegalStateException("DBF reader/writer is null, either you have opened a stream that can not be " +
                                "read from (a write-only stream) or you have not opened a stream at all.");


        // Move to the specified record, note that an exception will be thrown is stream is not seekable!
        // This is ok, since we provide a function to check whether the stream is seekable.
        long seekToPosition = header.HeaderLength() + (index * header.getRecordLength());

        // check whether requested record exists. Subtract 1 from file length (there is a terminating character 1A at the end of the file)
        // so if we hit end of file, there are no more records, so return false;
        if (index < 0 || dbfFile.length() - 1 <= seekToPosition) {
            return false;
        }

        // move to record and read
        dbfFile.seek(seekToPosition);

        // read the record
        boolean readRecord = fillRecord.read(dbfFile);
        if (readRecord) {
            fillRecord.setRecordIndex(index);
        }

        return readRecord;
    }

    /**
     * Reads a record specified by index. This method requires the stream to be able to seek to position.
     * If you are using a http stream, or a stream that can not stream, use readNext() methods to read in all records.
     *
     * @param index Zero based index.
     * @return Null if record can not be read, otherwise returns a new record.
     * @throws UnsupportedEncodingException If the encoding is not valid or is not supported.
     * @throws IOException If an I/O error occurs.
     */
    public DbfRecord read(int index) throws IOException {
        // create a new record and fill it.
        DbfRecord record = new DbfRecord(header);

        if (read(index, record)) {
            return record;
        }
        else {
            return null;
        }
    }

    /***
     * @param rowIndex Index of the row.
     * @param columnIndex Index of the column.
     * @param result Value that has been read
     * @return true if reading was successful, false otherwise
     * @throws IOException If an I/O error occurs.
     */
    public boolean readValue(int rowIndex, int columnIndex, StringBuilder result) throws IOException {
        result.delete(0, result.length());

        DbfColumn column = header.get(columnIndex);

        // move to the specified record, note that an exception will be thrown is stream is not seekable!
        // This is ok, since we provide a function to check whether the stream is seekable.
        long nSeekToPosition = header.HeaderLength() + (rowIndex * header.getRecordLength()) + column.getDataAddress();

        // check whether requested record exists. Subtract 1 from file length (there is a terminating character 1A at the end of the file)
        // so if we hit end of file, there are no more records, so return false;
        if (rowIndex < 0 || dbfFile.length() - 1 <= nSeekToPosition)
            return false;

        // move to position and read
        dbfFile.seek(nSeekToPosition);

        // read the value
        byte[] data = new byte[column.getLength()];
        dbfFile.read(data, 0, column.getLength());

        result.append(new String(data, 0, column.getLength(), Configuration.getEncodingName()));

        return true;
    }

    /**
     * Write a record to file. If RecordIndex is present, record will be updated, otherwise a new record will be written.
     * Header will be output first if this is the first record being written to file.
     * This method does NOT require stream seek capability to add a new record.
     * @param record
     * @throws IOException If an I/O error occurs.
     */
    public void write(DbfRecord record) throws IOException {
        // if header was never written, write it first, then output the record
        if (!headerWritten) {
            writeHeader();
        }

        // if this is a new record (RecordIndex should be -1 in that case)
        if (record.getRecordIndex() < 0) {
            if (!isForwardOnly) {
                /* Calculate number of records in file. Do not rely on header's RecordCount property since client can change that value.
                   Also note that some DBF files do not have ending 0x1A byte, so we subtract 1 and round off,
                   instead of just cast since cast would just drop decimals. */
                int nNumRecords = (int) Math.round(((double) (dbfFile.length() - header.HeaderLength() - 1) / header.getRecordLength()));

                if (nNumRecords < 0) {
                    nNumRecords = 0;
                }

                record.setRecordIndex(nNumRecords);
                update(record);
                header.setRecordCount(header.getRecordCount() + 1);
            } else {
                // we can not position this stream, just write out the new record.
                record.write(dbfFile);
                header.setRecordCount(header.getRecordCount() + 1);
            }
        } else {
            update(record);
        }
    }

    /***
     * Write a record to file. If RecordIndex is present, record will be updated, otherwise a new record will be written.
     * Header will be output first if this is the first record being written to file.
     * This method does NOT require stream seek capability to add a new record.     *
     * @param record Record to be added or updated.
     * @param clearRecordAfterWrite Clear all data in the record or not.
     * @throws IOException If an I/O error occurs.
     */
    public void write(DbfRecord record, boolean clearRecordAfterWrite) throws IOException {
        write(record);

        if (clearRecordAfterWrite) {
            record.clear();
        }
    }

    /**
     * Update a record. RecordIndex (zero based index) must be more than -1, otherwise an exception is thrown.
     * You can also use write method which updates a record if it has RecordIndex or adds a new one if RecordIndex == -1.
     * RecordIndex is set automatically when you call any read() methods on this class.
     *
     * @param record
     * @exception IllegalArgumentException If record index is not set.
     * @exception IllegalStateException If the DBF reader/writer is null.
     * @exception IndexOutOfBoundsException If trying to seek after the end of file.
     * @exception IllegalStateException If the size of the record does not match the size written in header.
     * @throws IOException IOException if an I/O error occurs while reading the file.
     */
    public void update(DbfRecord record) throws IOException {
        // if header was never written, write it first, then output the record
        if (!headerWritten) {
            writeHeader();
        }

        // Check if record has an index
        if (record.getRecordIndex() < 0) {
            throw new IllegalArgumentException("Record index is not set, unable to update record. " +
                    "Set record index or call write() method to add a new record to file.");
        }

        // Check if this record matches record size specified by header and number of columns.
        // Client can pass a record from another DBF that is incompatible with this one and that would corrupt the file.
        if (record.getHeader() != header && (record.getHeader().getColumnCount() != header.getColumnCount()
                                            || record.getHeader().getRecordLength() != header.getRecordLength()))
            throw new IllegalStateException("Record parameter does not have the same size and number of columns as the " +
                                "header specifies. Writing this record would corrupt the DBF file. " +
                                "This is a programming error, have you mixed up DBF file objects?");

        if (dbfFile == null) {
            throw new IllegalStateException("Write stream is null. Either a stream that can not be " +
                                "written to is opened (a read-only stream) or a stream has not been opened at all.");
        }


        // Move to the specified record, note that an exception will be thrown if stream is not seekable!
        // This is ok, since we provide a function to check whether the stream is seekable.
        long nSeekToPosition = (long) header.HeaderLength() + ((long) record.getRecordIndex() * (long) header.getRecordLength());

        //check whether we can seek to this position. Subtract 1 from file length (there is a terminating character 1A at the end of the file)
        //so if we hit end of file, there are no more records, so return false;
        if (dbfFile.length() < nSeekToPosition) {
            throw new IndexOutOfBoundsException("Invalid record position. Unable to save record.");
        }

        // move to record start
        dbfFile.seek(nSeekToPosition);

        // write
        record.write(dbfFile);
    }

    /**
     * Save header to file. Normally, you do not have to call this method, header is saved
     * automatically and updated when you close the file (if it has changed).
     * @return true if the header has been written successfully, false otherwise
     * @throws IOException IOException if an I/O error occurs while reading the file.
     */
    public boolean writeHeader() throws IOException {
        //update header if possible
        //--------------------------------
        if (dbfFile != null) {
            if (!isForwardOnly) {
                dbfFile.seek(0);
                header.write(dbfFile);
                headerWritten = true;
                return true;
            } else {
                //if stream can not seek, then just write it out and that's it.
                if (!headerWritten)  {
                    header.write(dbfFile);
                }
                headerWritten = true;
            }
        }

        return false;
    }

    /**
     * Access DBF header with information on columns. Use this object for faster access to header.
     * Remove one layer of function calls by saving header reference and using it directly to access columns.            *
     * @return DBF header.
     */
    public DbfHeader getHeader() {
        return header;
    }

}
