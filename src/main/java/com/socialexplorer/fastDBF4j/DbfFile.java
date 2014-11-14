package com.socialexplorer.fastDBF4j;

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
     * Helps read/write dbf file header information.
     */
    protected DbfHeader _header = new DbfHeader();

    /**
     * Flag that indicates whether the header was written or not...
     */
    protected boolean _headerWritten = false;

    /**
     * input and output stream
     */
    protected FileReader _dbfFile = null;

    /**
     * Path to the file, if the file was opened at all.
     */
    protected String _fileName = "";

    /**
     * Number of records read using readNext() methods only. This applies only when we are using a forward-only stream.
     * _recordsReadCount is used to keep track of record index. With a seek enabled stream,
     * we can always calculate index using stream position.
     */
    protected int _recordsReadCount = 0;

    /**
     * keep these values handy so we don't call functions on every read.
     */
    protected boolean _isForwardOnly = false;
    protected boolean _isReadOnly = false;

    /**
     * "r", "rw"
     */
    protected String _fileAccess = "";

    /**
     * Creates a DBF file object. Since there is no cpg file provided in this constructor, encoding will be read from
     * the dbf language driver ID. If there is no data in the language driver ID, windows-1252 will be used as default.
     *
     * @param filePath Full path to the file.
     * @param fileAccess read - "r", read/write - "rw"
     */
    public DbfFile(String filePath, String fileAccess) {
        _fileName = filePath;
        _fileAccess = fileAccess;

        Configuration.setEncodingName(Configuration.DEFAULT_ENCODING_NAME);
        Configuration.setShouldTryToSetEncodingFromLanguageDriver(true);
    }

    /**
     * @param filePath Full path to the file.
     * @param fileAccess read - "r", read/write - "rw"
     * @param cpgFilePath Path to the CPG file that contains encoding information on the first line.
     * @throws UnsupportedCharsetException If the encoding provided in the cpg file is not valid or not supported.
     * @throws IOException If the cpg file cannot be read.
     * @throws IllegalArgumentException If the CPG file is empty.
     */
    public DbfFile(String filePath, String fileAccess, String cpgFilePath) throws UnsupportedCharsetException, IOException {
        _fileName = filePath;
        _fileAccess = fileAccess;

        // Read cpg file
        List<String> lines = Files.readAllLines(Paths.get(cpgFilePath), StandardCharsets.UTF_8);
        if (lines.size() == 0) {
            throw new IllegalArgumentException("CPG file is empty. File path: " + cpgFilePath);
        }
        // Encoding should be on the first line.
        String encodingName = lines.get(0);

        // Set encoding to that given in cpg file, if possible.
        if (!Charset.isSupported(encodingName)) {
            throw new UnsupportedCharsetException("Encoding :" + encodingName + " not supported!");
        }
        Configuration.setEncodingName(encodingName);
        Configuration.setShouldTryToSetEncodingFromLanguageDriver(false);
    }

    /**
     * Open a DBF file or create a new one.
     *
     * @throws IOException
     * @throws FileNotFoundException
     * @throws Exception
     */
    public void open() throws IOException, FileNotFoundException, Exception {
        _recordsReadCount = 0; // reset position
        _headerWritten = false; // assume the header is not written
        _isForwardOnly = false; // RandomAccessFile can seek
        _isReadOnly = (_fileAccess.equals("r"));

        _dbfFile = new com.socialexplorer.util.FileReader(new RandomAccessFile(_fileName, _fileAccess));

        // read the header
        if (_fileAccess.contains("r")) {
            try {
                _header.read(_dbfFile);
                _headerWritten = true;
            } catch (EOFException e) {
                // could not read the header because file is empty
                _header = new DbfHeader();
                _headerWritten = false;
            }
        }
    }

    /**
     * Update header info, flush buffers and close streams. You should always call this method when you are done with a DBF file.
     *
     * @throws IOException
     * @throws Exception
     */
    public void close() throws IOException, Exception {
        // try to update the header if it has changed
        if (_header.getIsDirty()) {
            writeHeader();
        }

        // empty header
        _header = new DbfHeader();
        _headerWritten = false;

        // reset current record index
        _recordsReadCount = 0;

        // close stream
        if (_dbfFile != null) {
            _dbfFile.close();
        }
        _dbfFile = null;

        _fileName = "";
    }

    /**
     * @return true if we cannot write to the DBF file stream, false otherwise
     */
    public boolean isReadOnly() {
        return _isReadOnly;
    }

    /**
     * @return true if we cannot seek to different locations within the file, such as internet connections, false otherwise
     */
    public boolean isForwardOnly() {
        return _isForwardOnly;
    }

    /**
     * @return the name of the file stream
     */
    public String getFileName() {
        return _fileName;
    }

    /**
     * Read next record and fill data into parameter _fillRecord.
     *
     * @param fillRecord DbfRecord object into which the record is stored
     * @return true if a record was read, false otherwise
     * @throws Exception
     */
    public boolean readNext(DbfRecord fillRecord) throws Exception {
        // check if we can fill this record with data. it must match record size specified by header and number of columns.
        // we are not checking whether it comes from another DBF file or not, we just need the same structure. Allow flexibility but be safe.
        if (fillRecord.getHeader() != _header && (fillRecord.getHeader().getColumnCount() != _header.getColumnCount() || fillRecord.getHeader().getRecordLength() != _header.getRecordLength()))
            throw new Exception("Record parameter does not have the same size and number of columns as the " +
                                "header specifies, so we are unable to read a record into oFillRecord. " +
                                "This is a programming error, have you mixed up DBF file objects?");

        // DBF file reader can be null if stream is not readable.
        if (_dbfFile == null)
            throw new Exception("Read stream is null, either you have opened a stream that can not be " +
                                "read from (a write-only stream) or you have not opened a stream at all.");

        // read next record...
        boolean readSuccess = fillRecord.read(_dbfFile);

        if (readSuccess) {
            if (_isForwardOnly) {
                // zero based index! set before incrementing count.
                fillRecord.setRecordIndex(_recordsReadCount);
                _recordsReadCount++;
            } else {
                fillRecord.setRecordIndex(((int) ((_dbfFile.getFilePointer() - _header.HeaderLength()) / _header.getRecordLength())) - 1);
            }
        }

        return readSuccess;
    }

    /**
     * Reads the next record.
     *
     * @return a new record object or null if nothing was read
     * @throws Exception
     */
    public DbfRecord readNext() throws Exception {
        // create a new record and fill it
        DbfRecord nextRecord = new DbfRecord(_header);

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
     * @throws Exception
     */
    public boolean read(int index, DbfRecord fillRecord) throws Exception {
        // check if we can fill this record with data. it must match record size specified by header and number of columns.
        // we are not checking whether it comes from another DBF file or not, we just need the same structure. Allow flexibility but be safe.
        if (fillRecord.getHeader() != _header && (fillRecord.getHeader().getColumnCount() != _header.getColumnCount() || fillRecord.getHeader().getRecordLength() != _header.getRecordLength()))
            throw new Exception("Record parameter does not have the same size and number of columns as the " +
                                "header specifies, so we are unable to read a record into oFillRecord. " +
                                "This is a programming error, have you mixed up DBF file objects?");

        // DBF file reader can be null if stream is not readable...
        if (_dbfFile == null)
            throw new Exception("ReadStream is null, either you have opened a stream that can not be " +
                                "read from (a write-only stream) or you have not opened a stream at all.");


        // move to the specified record, note that an exception will be thrown is stream is not seekable!
        // This is ok, since we provide a function to check whether the stream is seekable.
        long seekToPosition = _header.HeaderLength() + (index * _header.getRecordLength());

        // check whether requested record exists. Subtract 1 from file length (there is a terminating character 1A at the end of the file)
        // so if we hit end of file, there are no more records, so return false;
        if (index < 0 || _dbfFile.length() - 1 <= seekToPosition) {
            return false;
        }

        // move to record and read
        _dbfFile.seek(seekToPosition);

        // read the record
        boolean readRecord = fillRecord.read(_dbfFile);
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
     * @throws Exception
     */
    public DbfRecord read(int index) throws Exception {
        // create a new record and fill it.
        DbfRecord record = new DbfRecord(_header);

        if (read(index, record)) {
            return record;
        }
        else {
            return null;
        }
    }

    /***
     * @param rowIndex
     * @param columnIndex
     * @param result
     * @return
     * @throws UnsupportedEncodingException
     * @throws IOException
     */
    public boolean readValue(int rowIndex, int columnIndex, StringBuilder result) throws UnsupportedEncodingException, IOException {
        result.delete(0, result.length());

        DbfColumn column = _header.get(columnIndex);

        // move to the specified record, note that an exception will be thrown is stream is not seekable!
        // This is ok, since we provide a function to check whether the stream is seekable.
        long nSeekToPosition = _header.HeaderLength() + (rowIndex * _header.getRecordLength()) + column.getDataAddress();

        // check whether requested record exists. Subtract 1 from file length (there is a terminating character 1A at the end of the file)
        // so if we hit end of file, there are no more records, so return false;
        if (rowIndex < 0 || _dbfFile.length() - 1 <= nSeekToPosition)
            return false;

        // move to position and read
        _dbfFile.seek(nSeekToPosition);

        // read the value
        byte[] data = new byte[column.getLength()];
        _dbfFile.read(data, 0, column.getLength());

        result.append(new String(data, 0, column.getLength(), Configuration.getEncodingName()));

        return true;
    }

    /**
     * Write a record to file. If RecordIndex is present, record will be updated, otherwise a new record will be written.
     * Header will be output first if this is the first record being written to file.
     * This method does NOT require stream seek capability to add a new record.
     *
     * @param record
     * @throws Exception
     */
    public void write(DbfRecord record) throws Exception {
        // if header was never written, write it first, then output the record
        if (!_headerWritten) {
            writeHeader();
        }

        // if this is a new record (RecordIndex should be -1 in that case)
        if (record.getRecordIndex() < 0) {
            if (!_isForwardOnly) {
                /* Calculate number of records in file. Do not rely on header's RecordCount property since client can change that value.
                   Also note that some DBF files do not have ending 0x1A byte, so we subtract 1 and round off,
                   instead of just cast since cast would just drop decimals. */
                int nNumRecords = (int) Math.round(((double) (_dbfFile.length() - _header.HeaderLength() - 1) / _header.getRecordLength()));

                if (nNumRecords < 0) {
                    nNumRecords = 0;
                }

                record.setRecordIndex(nNumRecords);
                update(record);
                _header.setRecordCount(_header.getRecordCount() + 1);
            } else {
                // we can not position this stream, just write out the new record.
                record.write(_dbfFile);
                _header.setRecordCount(_header.getRecordCount() + 1);
            }
        } else {
            update(record);
        }
    }

    /***
     * Write a record to file. If RecordIndex is present, record will be updated, otherwise a new record will be written.
     * Header will be output first if this is the first record being written to file.
     * This method does NOT require stream seek capability to add a new record.
     *
     * @param record Record to be added or updated.
     * @param clearRecordAfterWrite Clear all data in the record or not.
     * @throws Exception
     */
    public void write(DbfRecord record, boolean clearRecordAfterWrite) throws Exception {
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
     * @throws Exception
     */
    public void update(DbfRecord record) throws Exception {
        // if header was never written, write it first, then output the record
        if (!_headerWritten) {
            writeHeader();
        }

        // check if record has an index
        if (record.getRecordIndex() < 0) {
            throw new Exception("RecordIndex is not set, unable to update record. Set RecordIndex or call write() method to add a new record to file.");
        }

        // Check if this record matches record size specified by header and number of columns.
        // Client can pass a record from another DBF that is incompatible with this one and that would corrupt the file.
        if (record.getHeader() != _header && (record.getHeader().getColumnCount() != _header.getColumnCount() || record.getHeader().getRecordLength() != _header.getRecordLength()))
            throw new Exception("Record parameter does not have the same size and number of columns as the " +
                                "header specifies. Writing this record would corrupt the DBF file. " +
                                "This is a programming error, have you mixed up DBF file objects?");

        if (_dbfFile == null)
            throw new Exception("Write stream is null. Either you have opened a stream that can not be " +
                                "writen to (a read-only stream) or you have not opened a stream at all.");


        // move to the specified record, note that an exception will be thrown if stream is not seekable!
        // This is ok, since we provide a function to check whether the stream is seekable.
        long nSeekToPosition = (long) _header.HeaderLength() + (long) ((long) record.getRecordIndex() * (long) _header.getRecordLength());

        //check whether we can seek to this position. Subtract 1 from file length (there is a terminating character 1A at the end of the file)
        //so if we hit end of file, there are no more records, so return false;
        if (_dbfFile.length() < nSeekToPosition) {
            throw new Exception("Invalid record position. Unable to save record.");
        }

        // move to record start
        _dbfFile.seek(nSeekToPosition);

        // write
        record.write(_dbfFile);
    }

    /**
     * Save header to file. Normally, you do not have to call this method, header is saved
     * automatically and updated when you close the file (if it changed).
     *
     * @return
     * @throws Exception
     */
    public boolean writeHeader() throws Exception {
        //update header if possible
        //--------------------------------
        if (_dbfFile != null) {
            if (!_isForwardOnly) {
                _dbfFile.seek(0);
                _header.write(_dbfFile);
                _headerWritten = true;
                return true;
            } else {
                //if stream can not seek, then just write it out and that's it.
                if (!_headerWritten)  {
                    _header.write(_dbfFile);
                }
                _headerWritten = true;
            }
        }

        return false;
    }

    /**
     * Access DBF header with information on columns. Use this object for faster access to header.
     * Remove one layer of function calls by saving header reference and using it directly to access columns.
     *
     * @return DBF header.
     */
    public DbfHeader getHeader() {
        return _header;
    }

}
