package com.socialexplorer.fastDBF4j;

import com.socialexplorer.util.*;
import com.socialexplorer.util.FileReader;

import java.io.*;
//import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/***
 * Use this class to create a record and write it to a dbf file. You can use one record object to write all records!
 * It was designed for this kind of use. You can do this by clearing the record of all data
 * (call clear() method) or setting values to all fields again, then write to dbf file.
 * This eliminates creating and destroying objects and optimizes memory use.
 *
 * Once you create a record the header can no longer be modified, since modifying the header would make a corrupt DBF file.
 */
public class DbfRecord {
    private static final String CHARSET_NAME = "windows-1252";

    /***
     * Header provides information on all field types, sizes, precision and other useful information about the DBF.
     */
    private DbfHeader _header = null;

    /***
     * Dbf data are a mix of ASCII characters and binary, which neatly fit in a byte array.
     */
    private byte[] _data = null;

    /***
     * Zero based record index. -1 when not set, new records for example.
     */
    private int _recordIndex = -1;

    /***
     * Empty Record array reference used to clear fields quickly (or entire record).
     */
    private final byte[] _emptyRecord;

    /***
     * Specifies whether we allow strings to be truncated. If false and string
     * is longer than we can fit in the field, an exception is thrown.
     */
    private boolean _allowStringTruncate = true;

    /***
     * Specifies whether we allow the decimal portion of numbers to be truncated.
     * If false and decimal digits overflow the field, an exception is thrown.
     */
    private boolean _allowDecimalTruncate = false;

    /***
     * Specifies whether we allow the integer portion of numbers to be truncated.
     * If false and integer digits overflow the field, an exception is thrown.
     */
    private boolean _allowIntegerTruncate = false;

    /***
     * Array used to clear decimals, we can clear up to 40 decimals which is much more
     * than is allowed under DBF spec anyway.
     * Note: 48 is ASCII code for 0.
     */
    private static final byte[] _decimalClear = new byte[]{  48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48,
                                                                48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48,
                                                                48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48};

    /***
     *
     * @param header Dbf Header will be locked once a record is created since the record size
     *                is fixed and if the header was modified it would corrupt the DBF file.
     * @throws UnsupportedEncodingException
     */
    public DbfRecord(DbfHeader header) throws UnsupportedEncodingException {
        _header = header;
        _header.setLocked(true);

        // create a buffer to hold all record data. We will reuse this buffer to write all data to the file.
        _data = new byte[_header.getRecordLength()];
        _emptyRecord = _header.getEmptyDataRecord();
    }

    /***
     * Set string data to a column, if the string is longer than specified column length it will be truncated!
     * If dbf column type is not a string, input will be treated as dbf column
     * type and if longer than length an exception will be thrown.
     * @param colIndex
     * @param value
     * @throws Exception
     * @throws DbfDataTruncateException
     */
    public void set(int colIndex, String value) throws Exception, DbfDataTruncateException {
        DbfColumn column = _header.get(colIndex);
        DbfColumn.DbfColumnType columnType = column.getColumnType();

        // if an empty value is passed, we just clear the data, and leave it blank.
        // note: tests have shown that testing for null and checking length is faster than comparing to "" empty str :)
        if (value == null || value.length() == 0) {
            // this is like NULL data, set it to empty. i looked at SAS DBF output when a null value exists
            // and empty data are output. we get the same result, so this looks good.
            System.arraycopy(_emptyRecord, column.getDataAddress(), _data, column.getDataAddress(), column.getLength());
        } else {
            /**
             * --------------------------------------------------------
             * set value according to data type:
             * CHARACTER, NUMBER, INTEGER, MEMO, BOOLEAN, DATE, BINARY
             * --------------------------------------------------------
             */
            /**
             * -----------------------
             * CHARACTER
             * -----------------------
             */
            if (columnType == DbfColumn.DbfColumnType.CHARACTER) {
                if (!_allowStringTruncate && value.length() > column.getLength()) {
                    throw new DbfDataTruncateException("Value not set. String truncation would occur and AllowStringTruncate flag is set to false. To suppress this exception change AllowStringTruncate to true.");
                }

                // First clear the previous value, then set the new one.
                System.arraycopy(_emptyRecord, column.getDataAddress(), _data, column.getDataAddress(), column.getLength());

                int length = value.length() > column.getLength() ? column.getLength() : value.length();
                byte[] valueBytes = value.substring(0, length).getBytes(CHARSET_NAME);

                System.arraycopy(valueBytes, 0, _data, column.getDataAddress(), valueBytes.length);
            }
            /**
             * -----------------------
             * NUMBER
             * -----------------------
             */
            else if (columnType == DbfColumn.DbfColumnType.NUMBER) {
                if (column.getDecimalCount() == 0) {

                    //integers
                    //----------------------------------

                    //throw an exception if integer overflow would occur
                    if (!_allowIntegerTruncate && value.length() > column.getLength())
                        throw new DbfDataTruncateException("Value not set. Integer does not fit and would be truncated. AllowIntegerTruncate is set to false. To supress this exception set AllowIntegerTruncate to true, although that is not recomended.");

                    //clear all numbers, set to [space].
                    //-----------------------------------------------------
                    System.arraycopy(_emptyRecord, 0, _data, column.getDataAddress(), column.getLength());
                    //Buffer.BlockCopy(_emptyRecord, 0, _data, ocol.getDataAddress(), ocol.Length());


                    //set integer part, CAREFUL not to overflow buffer! (truncate instead)
                    //-----------------------------------------------------------------------
                    int nNumLen = value.length() > column.getLength() ? column.getLength() : value.length();
                    byte[] valueBytes = value.substring(0, nNumLen).getBytes(CHARSET_NAME);

                    System.arraycopy(valueBytes, 0, _data, (column.getDataAddress() + column.getLength() - nNumLen), valueBytes.length);
                    //ASCIIEncoder.GetBytes(value, 0, nNumLen, _data, (ocol.getDataAddress() + ocol.Length() - nNumLen));

                } else {
                    //break value down into integer and decimal portions
                    int indexDecimal = value.indexOf('.'); //index where the decimal point occurs
                    char[] cDec = null; //decimal portion of the number
                    char[] cNum = null; //integer portion

                    if (indexDecimal > -1) {
                        cDec = value.substring(indexDecimal + 1).trim().toCharArray();
                        cNum = value.substring(0, indexDecimal).toCharArray();

                        //throw an exception if decimal overflow would occur
                        if (!_allowDecimalTruncate && cDec.length > column.getDecimalCount())
                            throw new DbfDataTruncateException("Value not set. Decimal does not fit and would be truncated. AllowDecimalTruncate is set to false. To supress this exception set AllowDecimalTruncate to true.");

                    } else
                        cNum = value.toCharArray();


                    //throw an exception if integer overflow would occur
                    if (!_allowIntegerTruncate && cNum.length > column.getLength() - column.getDecimalCount() - 1)// -1 for the decimal point
                        throw new DbfDataTruncateException("Value not set. Integer does not fit and would be truncated. AllowIntegerTruncate is set to false. To supress this exception set AllowIntegerTruncate to true, although that is not recomended.");


                    //clear all decimals, set to 0.
                    //-----------------------------------------------------
                    System.arraycopy(_decimalClear, 0, _data, (column.getDataAddress() + column.getLength() - column.getDecimalCount()), column.getDecimalCount());
                    //clear all numbers, set to [space].
                    System.arraycopy(_emptyRecord, 0, _data, column.getDataAddress(), (column.getLength() - column.getDecimalCount()));

                    //set decimal numbers, CAREFUL not to overflow buffer! (truncate instead)
                    if (indexDecimal > -1) {
                        int decimalLength = cDec.length > column.getDecimalCount() ? column.getDecimalCount() : cDec.length;
                        byte[] valueBytes = value.substring(value.indexOf('.') + 1, value.indexOf('.') + decimalLength + 1).getBytes(CHARSET_NAME);
                        System.arraycopy(valueBytes, 0, _data, (column.getDataAddress() + column.getLength() - column.getDecimalCount()), valueBytes.length);
                    }

                    //set integer part, CAREFUL not to overflow buffer! (truncate instead)
                    //-----------------------------------------------------------------------
                    int nNumLen = cNum.length > column.getLength() - column.getDecimalCount() - 1 ? (column.getLength() - column.getDecimalCount() - 1) : cNum.length;
                    byte[] valueBytes = value.substring(0, nNumLen).getBytes(CHARSET_NAME);
                    System.arraycopy(valueBytes, 0, _data, (column.getDataAddress() + column.getLength() - column.getDecimalCount() - nNumLen - 1), valueBytes.length);

                    //set decimal point
                    //-----------------------------------------------------------------------
                    _data[column.getDataAddress() + column.getLength() - column.getDecimalCount() - 1] = (byte) '.';
                }
            }
            /**
             * -----------------------
             * INTEGER
             * -----------------------
             */
            else if (columnType == DbfColumn.DbfColumnType.INTEGER) {
                // NOTE: INTEGER types are written as BINARY 4-byte LITTLE endian integers

                int intValue = Integer.parseInt(value);
                intValue = ByteUtils.swap(intValue); // to get little endian

                byte[] valueBytes = ByteUtils.int2byte(intValue);
                System.arraycopy(valueBytes, 0, _data, column.getDataAddress(), 4);

            }
            /**
             * -----------------------
             * MEMO
             * -----------------------
             */
            else if (columnType == DbfColumn.DbfColumnType.MEMO) {
                // copy 10 digits...
                // TODO: implement MEMO
                throw new Exception("Memo data type functionality not implemented yet!");
            }
            /**
             * -----------------------
             * BOOLEAN
             * -----------------------
             */
            else if (columnType == DbfColumn.DbfColumnType.BOOLEAN) {
                if (value.toLowerCase().equals("true")   || value.toLowerCase().equals("1") ||
                    value.toUpperCase().equals("T")      || value.toLowerCase().equals("yes") ||
                    value.toUpperCase().equals("Y")) {
                    _data[column.getDataAddress()] = (byte) 'T';
                }
                else if (value.equals(" ") || value.equals("?")) {
                    _data[column.getDataAddress()] = (byte) '?';
                }
                else {
                    _data[column.getDataAddress()] = (byte) 'F';
                }

            }
            /**
             * -----------------------
             * DATE
             * -----------------------
             */
            else if (columnType == DbfColumn.DbfColumnType.DATE) {
                // try to parse out date value using Date.Parse() function, then set the value
                Date dateVal;
                SimpleDateFormat sdf = new SimpleDateFormat();

                try {
                    dateVal = SimpleDateFormat.getInstance().parse(value);
                    setDateValue(colIndex, dateVal);
                }
                catch(Exception e) {
                    throw new Exception("Date could not be parsed from source string! Please parse the Date and set the value.");
                }

            }
            /**
             * -----------------------
             * BINARY
             * -----------------------
             */
            else if (columnType == DbfColumn.DbfColumnType.BINARY) {
                throw new Exception("Cannot use string source to set binary data. Use setBinaryValue() and getBinaryValue() functions instead.");
            }
            else {
                throw new Exception("Unrecognized data type: " + columnType.toString());
            }
        }
    }

    public String get(int colIndex) throws UnsupportedEncodingException, IOException {
        DbfColumn column = _header.get(colIndex);
        String val = "";

        // NOTE: integer types are written as BINARY - 4-byte little endian integers
        if (column.getColumnType() == DbfColumn.DbfColumnType.INTEGER) {
            byte[] intBytes = new byte[4];
            System.arraycopy(_data, column.getDataAddress(), intBytes, 0, 4);

            val = Integer.toString(ByteUtils.swap(ByteUtils.byte2int(intBytes)));  // swap to get big endian
        } else {
            val = new String(_data, column.getDataAddress(), column.getLength(), CHARSET_NAME);
        }
        return val;
    }

    /***
     * Get date value.
     *
     * @param nColIndex
     * @return
     * @throws Exception
     */
    public Date getDateValue(int nColIndex) throws Exception {
        DbfColumn ocol = _header.get(nColIndex);

        if (ocol.getColumnType() == DbfColumn.DbfColumnType.DATE) {
            String sDateVal = new String(_data, ocol.getDataAddress(), ocol.getLength(), CHARSET_NAME); //ASCIIEncoder.GetString(_data, ocol.getDataAddress(), ocol.Length());
            SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
            return format.parse(sDateVal);
            //return DateTime.ParseExact(sDateVal, "yyyyMMdd", CultureInfo.InvariantCulture);
        } else {
            throw new Exception("Invalid data type. Column '" + ocol.getName() + "' is not a date column.");
        }
    }

    /***
     *  Set date value.
     *
     * @param nColIndex
     * @param value
     * @throws Exception
     */
    public void setDateValue(int nColIndex, Date value) throws Exception {
        DbfColumn column = _header.get(nColIndex);
        DbfColumn.DbfColumnType columnType = column.getColumnType();


        if (columnType == DbfColumn.DbfColumnType.DATE) {
            // Format date and set value. Date format is: yyyyMMdd
            String formattedValue = (new SimpleDateFormat("yyyyMMdd")).format(value);
            byte[] bytes = formattedValue.substring(0, column.getLength()).getBytes(CHARSET_NAME);

            System.arraycopy(bytes, 0, _data, column.getDataAddress(), bytes.length);
            //ASCIIEncoder.GetBytes(value.toString("yyyyMMdd"), 0, ocol.Length(), _data, ocol.getDataAddress());
        }
        else {
            throw new Exception("Invalid data type. Column is of '" + column.getColumnType().toString() + "' type, not date.");
        }
    }

    /***
     * Clears all data in the record.
     */
    public void clear() {
        System.arraycopy(_emptyRecord, 0, _data, 0, _emptyRecord.length);
        //Buffer.BlockCopy(_emptyRecord, 0, _data, 0, _emptyRecord.length);
        _recordIndex = -1;
    }

    /***
     * Returns a string representation of this record.
     * @return
     */
    public String ToString() throws UnsupportedEncodingException {
        return new String(_data, CHARSET_NAME);
    }

    /***
     * Gets/sets a zero based record index. This information is not directly stored in DBF.
     * It is the location of this record within the DBF.
     /// This property is managed from outside this object,
     /// CDbfFile object updates it when records are read. The reason we don't set it in the read()
     /// function within this object is that the stream can be forward-only so the Position property
     /// is not available and there is no way to figure out what index the record was unless you
     /// count how many records were read, and that's exactly what CDbfFile does.
     * @return
     */
    public int getRecordIndex() {
        return _recordIndex;
    }

    public void setRecordIndex(int recordIndex) {
        _recordIndex = recordIndex;
    }

    /***
     * Returns/sets flag indicating whether this record was tagged deleted.
     * Use CDbf4File.Compress() function to rewrite dbf removing records flagged as deleted.
     * @return
     */
    public boolean getIsDeleted() {
        return _data[0] == '*';
    }

    public void setIsDeleted(boolean isDeleted) {
        _data[0] = isDeleted ? (byte) '*' : (byte) ' ';
    }

    /***
     * Specifies whether strings can be truncated. If false and string is longer than can fit in the field, an exception is thrown.
     * Default is True.
     * @return
     */
    public boolean getAllowStringTruncate() {
        return _allowStringTruncate;
    }

    public void setAllowStringTruncate(boolean allowStringTruncate) {
        _allowStringTruncate = allowStringTruncate;
    }

    /***
     * Specifies whether to allow the decimal portion of numbers to be truncated.
     * If false and decimal digits overflow the field, an exception is thrown. Default is false.
     *
     * @return
     */
    public boolean getAllowDecimalTruncate() {
        return _allowDecimalTruncate;
    }

    public void setAllowDecimalTruncate(boolean allowDecimalTruncate) {
        _allowDecimalTruncate = allowDecimalTruncate;
    }

    /***
     * Specifies whether integer portion of numbers can be truncated.
     * If false and integer digits overflow the field, an exception is thrown.
     * Default is False.
     * @return
     */
    public boolean getAllowIntegerTruncate() {
        return _allowIntegerTruncate;
    }

    public void setAllowIntegerTruncate(boolean allowIntegerTruncate) {
        _allowIntegerTruncate = allowIntegerTruncate;
    }

    /***
     * Returns header object associated with this record.
     *
     * @return
     */
    public DbfHeader getHeader() {
        return _header;
    }

    /***
     * Get column by index
     * @param index
     * @return
     */
    public DbfColumn getColumn(int index) {
        return _header.get(index);
    }

    /// <summary>
    /// Get column by name.
    /// </summary>
    /// <param name="index"></param>
    /// <returns></returns>
   /* public com.socialexplorer.geoservices.fastDbf.DbfColumn Column(String sName) {
        return _header.get(sName);
    }                   */

    /***
     * Gets column count from header.
     * @return column count from header
     */
    public int getColumnCount() {
        return _header.getColumnCount();
    }

    /// <summary>
    /// Finds a column index by searching sequentially through the list. Case is ignored. Returns -1 if not found.
    /// </summary>
    /// <param name="sName">Column name.</param>
    /// <returns>Column index (0 based) or -1 if not found.</returns>
    /*public int FindColumn(String sName) {
        return _header.FindColumn(sName);
    }               */

    /***
     * Writes data to stream. Make sure stream is positioned correctly because we simply write out the data to it.
     * @param dbfFile
     * @throws Exception
     */
    protected void write(FileReader dbfFile) throws Exception {
        dbfFile.write(_data, 0, _data.length);
    }

    /***
     * Writes data to stream. Make sure stream is positioned correctly because we simply write out data to it, and clear the record.
     * @param dbfFile
     * @param clearRecordAfterWrite
     * @throws Exception
     */
    protected void write(FileReader dbfFile, boolean clearRecordAfterWrite) throws Exception {
        dbfFile.write(_data, 0, _data.length);

        if (clearRecordAfterWrite) {
            clear();
        }
    }

    /***
     * Read record from stream. Returns true if record read completely, otherwise returns false.
     * @param dbfFile Input data stream.
     * @return true if record was read, false otherwise (there is no more data in the stream)
     * @throws Exception
     */
    protected boolean read(FileReader dbfFile) throws Exception {
        if(dbfFile.read(_data, 0, _data.length) < _data.length) {
            return false;
        }
        return true;
    }

    protected String readValue(com.socialexplorer.util.FileReader dbfFile, int columnIndex) throws UnsupportedEncodingException {
        DbfColumn column = _header.get(columnIndex);
        return new String(_data, column.getDataAddress(), column.getLength(), CHARSET_NAME);
    }

    public void setData(byte[] data) {
        this._data = data;
    }

    public void setHeader(DbfHeader header) {
        this._header = header;
    }

}