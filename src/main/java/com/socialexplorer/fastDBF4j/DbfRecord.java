package com.socialexplorer.fastDBF4j;

import com.socialexplorer.fastDBF4j.exceptions.DbfDataTruncateException;
import com.socialexplorer.fastDBF4j.util.ByteUtils;
import com.socialexplorer.fastDBF4j.util.Configuration;
import com.socialexplorer.fastDBF4j.util.FileReader;
import org.apache.commons.lang.NotImplementedException;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
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
    /***
     * Header provides information on all field types, sizes, precision and other useful information about the DBF.
     */
    private DbfHeader header = null;

    /***
     * Dbf data are a mix of ASCII characters and binary, which neatly fit in a byte array.
     */
    private byte[] data = null;

    /***
     * Zero based record index. -1 when not set, new records for example.
     */
    private int recordIndex = -1;

    /***
     * Empty Record array reference used to clear fields quickly (or entire record).
     */
    private final byte[] emptyRecord;

    /***
     * Specifies whether we allow strings to be truncated. If false and string
     * is longer than we can fit in the field, an exception is thrown.
     */
    private boolean allowStringTruncate = true;

    /***
     * Specifies whether we allow the decimal portion of numbers to be truncated.
     * If false and decimal digits overflow the field, an exception is thrown.
     */
    private boolean allowDecimalTruncate = false;

    /***
     * Specifies whether we allow the integer portion of numbers to be truncated.
     * If false and integer digits overflow the field, an exception is thrown.
     */
    private boolean allowIntegerTruncate = false;

    /***
     * Array used to clear decimals, we can clear up to 40 decimals which is much more
     * than is allowed under DBF spec anyway.
     * Note: 48 is ASCII code for 0.
     */
    private static final byte[] DECIMAL_CLEAR = new byte[] {48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48,
                                                                48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48,
                                                                48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48, 48};

    /***
     *
     * @param header Dbf Header will be locked once a record is created since the record size
     *                is fixed and if the header was modified it would corrupt the DBF file.
     * @throws UnsupportedEncodingException If the named charset is not supported
     */
    public DbfRecord(DbfHeader header) throws UnsupportedEncodingException {
        this.header = header;
        this.header.setLocked(true);

        // create a buffer to hold all record data. We will reuse this buffer to write all data to the file.
        data = new byte[this.header.getRecordLength()];
        emptyRecord = this.header.getEmptyDataRecord();
    }

    /***
     * Set string data to a column, if the string is longer than specified column length it will be truncated!
     * If dbf column type is not a string, input will be treated as dbf column
     * type and if longer than length an exception will be thrown.
     * @param colIndex Index of the column.
     * @param value Value of the column in this record.
     * @exception NotImplementedException If trying to use MEMO column.
     * @exception IllegalArgumentException If trying to parse a date and the value is not a valid date.
     * @exception UnsupportedEncodingException If the encoding is not supported or is not valid.
     * @exception UnsupportedOperationException If trying to set binary column using a string value.
     * @exception UnsupportedOperationException If column type is not supported.
     * @throws IOException If an I/O error occurs.
     * @throws DbfDataTruncateException If value length exceeds column length and string truncating is not allowed.
     */
    public void set(int colIndex, String value) throws DbfDataTruncateException, IOException {
        DbfColumn column = header.get(colIndex);
        DbfColumn.DbfColumnType columnType = column.getColumnType();

        // if an empty value is passed, we just clear the data, and leave it blank.
        // note: tests have shown that testing for null and checking length is faster than comparing to "" empty str :)
        if (value == null) {
            String nullValue = columnType.getNullValue();
            if (nullValue == null) {
                System.arraycopy(emptyRecord, column.getDataAddress(), data, column.getDataAddress(), column.getLength());
            } else {
                byte[] valueBytes = nullValue.getBytes(header.getConfiguration().getEncodingName());

                if (valueBytes.length > column.getLength()) {
                    throw new DbfDataTruncateException("Trying to write null value as: " + nullValue + " but it exceeds column length.");
                }
                // First clear the previous value, then set the new one.
                System.arraycopy(emptyRecord, column.getDataAddress(), data, column.getDataAddress(), column.getLength());
                System.arraycopy(valueBytes, 0, data, column.getDataAddress(), valueBytes.length);
            }
        } else if (value.length() == 0) {
            // This is like NULL data, set it to empty. I looked at SAS DBF output when a null value exists
            // and empty data are output. we get the same result, so this looks good.
            System.arraycopy(emptyRecord, column.getDataAddress(), data, column.getDataAddress(), column.getLength());
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
                int valueByteLength = value.getBytes(header.getConfiguration().getEncodingName()).length;

                if (!allowStringTruncate && valueByteLength > column.getLength()) {
                    throw new DbfDataTruncateException("Value exceeds column length. String truncation would occur " +
                            "and AllowStringTruncate flag is set to false. " +
                            "To suppress this exception change AllowStringTruncate to true.");
                }

                // First clear the previous value, then set the new one.
                System.arraycopy(emptyRecord, column.getDataAddress(), data, column.getDataAddress(), column.getLength());

                int trimmedValueLength = valueByteLength > column.getLength() ? column.getLength() : valueByteLength;
                byte[] valueBytes = value.getBytes(header.getConfiguration().getEncodingName());

                System.arraycopy(valueBytes, 0, data, column.getDataAddress(), trimmedValueLength);
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
                    if (!allowIntegerTruncate && value.length() > column.getLength()) {
                        throw new DbfDataTruncateException("Value not set. Integer does not fit and would be truncated. " +
                                "AllowIntegerTruncate is set to false. To suppress this exception set allowIntegerTruncate " +
                                "to true, although that is not recommended.");
                    }

                    //clear all numbers, set to [space].
                    //-----------------------------------------------------
                    System.arraycopy(emptyRecord, 0, data, column.getDataAddress(), column.getLength());
                    //Buffer.BlockCopy(emptyRecord, 0, data, ocol.getDataAddress(), ocol.Length());


                    //set integer part, CAREFUL not to overflow buffer! (truncate instead)
                    //-----------------------------------------------------------------------
                    int nNumLen = value.length() > column.getLength() ? column.getLength() : value.length();
                    byte[] valueBytes = value.substring(0, nNumLen).getBytes(header.getConfiguration().getEncodingName());

                    System.arraycopy(valueBytes, 0, data, (column.getDataAddress() + column.getLength() - nNumLen), valueBytes.length);
                } else {
                    //break value down into integer and decimal portions
                    int indexDecimal = value.indexOf('.'); //index where the decimal point occurs
                    char[] cDec = null; //decimal portion of the number
                    char[] cNum = null; //integer portion

                    if (indexDecimal > -1) {
                        cDec = value.substring(indexDecimal + 1).trim().toCharArray();
                        cNum = value.substring(0, indexDecimal).toCharArray();

                        //throw an exception if decimal overflow would occur
                        if (!allowDecimalTruncate && cDec.length > column.getDecimalCount())
                            throw new DbfDataTruncateException("Value not set. Decimal does not fit and would be truncated. " +
                                    "AllowDecimalTruncate is set to false. " +
                                    "To suppress this exception set AllowDecimalTruncate to true.");

                    } else
                        cNum = value.toCharArray();


                    //throw an exception if integer overflow would occur
                    if (!allowIntegerTruncate && cNum.length > column.getLength() - column.getDecimalCount() - 1)// -1 for the decimal point
                        throw new DbfDataTruncateException("Value not set. Integer does not fit and would be truncated. " +
                                "AllowIntegerTruncate is set to false. " +
                                "To suppress this exception set AllowIntegerTruncate to true, although that is not recommended.");


                    // Clear all decimals, set to 0.
                    //-----------------------------------------------------
                    System.arraycopy(DECIMAL_CLEAR, 0, data, (column.getDataAddress() + column.getLength() - column.getDecimalCount()), column.getDecimalCount());
                    //clear all numbers, set to [space].
                    System.arraycopy(emptyRecord, 0, data, column.getDataAddress(), (column.getLength() - column.getDecimalCount()));

                    //set decimal numbers, CAREFUL not to overflow buffer! (truncate instead)
                    if (indexDecimal > -1) {
                        int decimalLength = cDec.length > column.getDecimalCount() ? column.getDecimalCount() : cDec.length;
                        byte[] valueBytes = value.substring(value.indexOf('.') + 1, value.indexOf('.') + decimalLength + 1).getBytes(header.getConfiguration().getEncodingName());
                        System.arraycopy(valueBytes, 0, data, (column.getDataAddress() + column.getLength() - column.getDecimalCount()), valueBytes.length);
                    }

                    //set integer part, CAREFUL not to overflow buffer! (truncate instead)
                    //-----------------------------------------------------------------------
                    int nNumLen = cNum.length > column.getLength() - column.getDecimalCount() - 1 ? (column.getLength() - column.getDecimalCount() - 1) : cNum.length;
                    byte[] valueBytes = value.substring(0, nNumLen).getBytes(header.getConfiguration().getEncodingName());
                    System.arraycopy(valueBytes, 0, data, (column.getDataAddress() + column.getLength() - column.getDecimalCount() - nNumLen - 1), valueBytes.length);

                    //set decimal point
                    //-----------------------------------------------------------------------
                    data[column.getDataAddress() + column.getLength() - column.getDecimalCount() - 1] = (byte) '.';
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
                System.arraycopy(valueBytes, 0, data, column.getDataAddress(), 4);

            }
            /**
             * -----------------------
             * MEMO
             * -----------------------
             */
            else if (columnType == DbfColumn.DbfColumnType.MEMO) {
                // copy 10 digits TODO
                throw new NotImplementedException("Memo data type functionality not implemented yet!");
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
                    data[column.getDataAddress()] = (byte) 'T';
                }
                else if (value.equals(" ") || value.equals("?")) {
                    data[column.getDataAddress()] = (byte) '?';
                }
                else {
                    data[column.getDataAddress()] = (byte) 'F';
                }

            }
            /**
             * -----------------------
             * DATE
             * -----------------------
             */
            else if (columnType == DbfColumn.DbfColumnType.DATE) {
                // Try to parse out date value then set the value.
                Date dateVal;
                try {
                    dateVal = SimpleDateFormat.getInstance().parse(value);
                    setDateValue(colIndex, dateVal);
                } catch(Exception e) {
                    throw new IllegalArgumentException("Date could not be parsed from source string.");
                }

            }
            /**
             * -----------------------
             * BINARY
             * -----------------------
             */
            else if (columnType == DbfColumn.DbfColumnType.BINARY) {
                throw new UnsupportedOperationException("Cannot use string source to set binary data. Use setBinaryValue() and getBinaryValue() functions instead.");
            } else {
                throw new UnsupportedOperationException("Unrecognized data type: " + columnType.toString());
            }
        }
    }

    /**
     * @param colIndex
     * @return
     * @throws UnsupportedEncodingException If the set encoding is not supported or valid.
     * @exception IOException If an I/O error occurs while trying to swap integer bytes from little to big endian.
     */
    public String get(int colIndex) throws IOException {
        DbfColumn column = header.get(colIndex);
        String val = "";

        // NOTE: integer types are written as BINARY - 4-byte little endian integers
        if (column.getColumnType() == DbfColumn.DbfColumnType.INTEGER) {
            byte[] intBytes = new byte[4];
            System.arraycopy(data, column.getDataAddress(), intBytes, 0, 4);

            val = Integer.toString(ByteUtils.swap(ByteUtils.byte2int(intBytes))); // swap to get big endian
        } else {
            val = new String(data, column.getDataAddress(), column.getLength(), header.getConfiguration().getEncodingName());

            if (column.getColumnType().isNullValue(val)) val = "";
        }

        return val;
    }

    /***
     * Get date value.
     * @param columnIndex Index of the column.
     * @return Date in column with index of the current record.
     * @throws UnsupportedEncodingException If the set encoding is not supported or valid.
     * @throws ParseException If trying to parse invalid date string.
     * @exception UnsupportedOperationException If trying to set date in a column that is not of date type
     */
    public Date getDateValue(int columnIndex) throws UnsupportedEncodingException, ParseException {
        DbfColumn column = header.get(columnIndex);

        if (column.getColumnType() == DbfColumn.DbfColumnType.DATE) {
            String sDateVal = new String(data, column.getDataAddress(), column.getLength(), header.getConfiguration().getEncodingName());
            SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
            return format.parse(sDateVal);
        } else {
            throw new UnsupportedOperationException("Invalid data type. Column '" + column.getName() + "' is not a date column.");
        }
    }

    /**
     * Set date value.
     * @param nColIndex Index of the column in which to set the date.
     * @param value Date value as string.
     * @throws UnsupportedEncodingException If the set encoding is not supported or valid.
     * @exception UnsupportedOperationException If trying to set date in a column that is not of date type.
     */
    public void setDateValue(int nColIndex, Date value) throws UnsupportedEncodingException {
        DbfColumn column = header.get(nColIndex);
        DbfColumn.DbfColumnType columnType = column.getColumnType();

        if (columnType == DbfColumn.DbfColumnType.DATE) {
            // Format date and set value. Date format is: yyyyMMdd
            String formattedValue = (new SimpleDateFormat("yyyyMMdd")).format(value);
            byte[] bytes = formattedValue.substring(0, column.getLength()).getBytes(header.getConfiguration().getEncodingName());

            System.arraycopy(bytes, 0, data, column.getDataAddress(), bytes.length);
        } else {
            throw new UnsupportedOperationException("Invalid data type. Column is of '" + column.getColumnType().toString() + "' type, not date.");
        }
    }

    /***
     * Clears all data in the record.
     */
    public void clear() {
        System.arraycopy(emptyRecord, 0, data, 0, emptyRecord.length);
        recordIndex = -1;
    }

    /***
     * Returns a string representation of this record.
     * @return Entire record converted to string.
     * @throws UnsupportedEncodingException If the set encoding is not supported or valid.
     */
    public String toString() {
        try {
            return new String(data, header.getConfiguration().getEncodingName());
        } catch(UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
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
        return recordIndex;
    }

    public void setRecordIndex(int recordIndex) {
        this.recordIndex = recordIndex;
    }

    /***
     * Returns/sets flag indicating whether this record was tagged deleted.
     * Use CDbf4File.Compress() function to rewrite dbf removing records flagged as deleted.
     * @return
     */
    public boolean getIsDeleted() {
        return data[0] == '*';
    }

    public void setIsDeleted(boolean isDeleted) {
        data[0] = isDeleted ? (byte) '*' : (byte) ' ';
    }

    /***
     * Specifies whether strings can be truncated. If false and string is longer than can fit in the field, an exception is thrown.
     * Default is True.
     * @return
     */
    public boolean getAllowStringTruncate() {
        return allowStringTruncate;
    }

    public void setAllowStringTruncate(boolean allowStringTruncate) {
        this.allowStringTruncate = allowStringTruncate;
    }

    /***
     * Specifies whether to allow the decimal portion of numbers to be truncated.
     * If false and decimal digits overflow the field, an exception is thrown. Default is false.
     *
     * @return
     */
    public boolean getAllowDecimalTruncate() {
        return allowDecimalTruncate;
    }

    public void setAllowDecimalTruncate(boolean allowDecimalTruncate) {
        this.allowDecimalTruncate = allowDecimalTruncate;
    }

    /***
     * Specifies whether integer portion of numbers can be truncated.
     * If false and integer digits overflow the field, an exception is thrown.
     * Default is False.
     * @return
     */
    public boolean getAllowIntegerTruncate() {
        return allowIntegerTruncate;
    }

    public void setAllowIntegerTruncate(boolean allowIntegerTruncate) {
        this.allowIntegerTruncate = allowIntegerTruncate;
    }

    /***
     * Returns header object associated with this record.
     *
     * @return
     */
    public DbfHeader getHeader() {
        return header;
    }

    /***
     * Get column by index
     * @param index
     * @return
     */
    public DbfColumn getColumn(int index) {
        return header.get(index);
    }

    /**
     * Get column by name.
     * @param name
     * @return
     */
    public DbfColumn getColumn(String name) {
        return header.getColumn(name);
    }

    /***
     * Gets column count from header.
     * @return column count from header
     */
    public int getColumnCount() {
        return header.getColumnCount();
    }

    /**
     * Finds a column index by searching sequentially through the list. Case is ignored. Returns -1 if not found.
     * @param columnName Column name.
     * @return Column index (0 based) or -1 if not found
     */
    public int getColumnIndex(String columnName) {
        return header.findColumn(columnName);
    }

    /***
     * Writes data to stream. Make sure stream is positioned correctly because we simply write out the data to it.
     * @param dbfFile
     * @throws IOException If an I/O error occurs.
     */
    protected void write(FileReader dbfFile) throws IOException {
        dbfFile.write(data, 0, data.length);
    }

    /***
     * Writes data to stream. Make sure stream is positioned correctly because we simply write out data to it, and clear the record.
     * @param dbfFile File writer.
     * @param clearRecordAfterWrite If true, clears this records data after write, does nothing otherwise.
     * @throws IOException If an I/O error occurs.
     */
    protected void write(FileReader dbfFile, boolean clearRecordAfterWrite) throws IOException {
        dbfFile.write(data, 0, data.length);

        if (clearRecordAfterWrite) {
            clear();
        }
    }

    /***
     * Read record. Returns true if record read completely, otherwise returns false.
     * @param dbfFile Input data stream.
     * @return true if record was read, false otherwise (there is no more data in the stream)
     * @throws IOException If an I/O error occurs.
     */
    protected boolean read(FileReader dbfFile) throws IOException {
        if(dbfFile.read(data, 0, data.length) < data.length) {
            return false;
        }
        return true;
    }

    /**
     * Read value from the given column from this record.
     * @param columnIndex Index of the column to be read.
     * @return Value of the given column in this record.
     * @throws UnsupportedEncodingException If the encoding is not valid or is not supported.
     */
    protected String readValue(int columnIndex) throws UnsupportedEncodingException {
        DbfColumn column = header.get(columnIndex);
        return new String(data, column.getDataAddress(), column.getLength(), header.getConfiguration().getEncodingName());
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public void setHeader(DbfHeader header) {
        this.header = header;
    }

}