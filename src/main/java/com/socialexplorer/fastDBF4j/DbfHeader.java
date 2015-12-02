package com.socialexplorer.fastDBF4j;

import com.socialexplorer.fastDBF4j.exceptions.InvalidDbfFileException;
import com.socialexplorer.fastDBF4j.util.ByteUtils;
import com.socialexplorer.fastDBF4j.util.Configuration;
import com.socialexplorer.fastDBF4j.util.FileReader;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * /// This class represents a DBF IV file header.
 * ///
 * /// DBF files are really wasteful on space but this legacy format lives on because it's really really simple.
 * /// It lacks much in features though.
 * ///
 * ///
 * /// Thanks to Erik Bachmann for providing the DBF file structure information!!
 * /// http://www.clicketyclick.dk/databases/xbase/format/dbf.html
 * ///
 * ///           _______________________  _______
 * /// 00h /   0| Version number      *1|  ^
 * ///          |-----------------------|  |
 * /// 01h /   1| Date of last update   |  |
 * /// 02h /   2|      YYMMDD        *21|  |
 * /// 03h /   3|                    *14|  |
 * ///          |-----------------------|  |
 * /// 04h /   4| Number of records     | Record
 * /// 05h /   5| in data file          | header
 * /// 06h /   6| ( 32 bits )        *14|  |
 * /// 07h /   7|                       |  |
 * ///          |-----------------------|  |
 * /// 08h /   8| Length of header   *14|  |
 * /// 09h /   9| structure ( 16 bits ) |  |
 * ///          |-----------------------|  |
 * /// 0Ah /  10| Length of each record |  |
 * /// 0Bh /  11| ( 16 bits )     *2 *14|  |
 * ///          |-----------------------|  |
 * /// 0Ch /  12| ( Reserved )        *3|  |
 * /// 0Dh /  13|                       |  |
 * ///          |-----------------------|  |
 * /// 0Eh /  14| Incomplete transac.*12|  |
 * ///          |-----------------------|  |
 * /// 0Fh /  15| Encryption flag    *13|  |
 * ///          |-----------------------|  |
 * /// 10h /  16| Free record thread    |  |
 * /// 11h /  17| (reserved for LAN     |  |
 * /// 12h /  18|  only )               |  |
 * /// 13h /  19|                       |  |
 * ///          |-----------------------|  |
 * /// 14h /  20| ( Reserved for        |  |            _        |=======================| ______
 * ///          |   multi-user dBASE )  |  |           / 00h /  0| Field name in ASCII   |  ^
 * ///          : ( dBASE III+ - )      :  |          /          : (terminated by 00h)   :  |
 * ///          :                       :  |         |           |                       |  |
 * /// 1Bh /  27|                       |  |         |   0Ah / 10|                       |  |
 * ///          |-----------------------|  |         |           |-----------------------| For
 * /// 1Ch /  28| MDX flag (dBASE IV)*14|  |         |   0Bh / 11| Field type (ASCII) *20| each
 * ///          |-----------------------|  |         |           |-----------------------| field
 * /// 1Dh /  29| Language driver     *5|  |        /    0Ch / 12| Field data address    |  |
 * ///          |-----------------------|  |       /             |                     *6|  |
 * /// 1Eh /  30| ( Reserved )          |  |      /              | (in memory !!!)       |  |
 * /// 1Fh /  31|                     *3|  |     /       0Fh / 15| (dBASE III+)          |  |
 * ///          |=======================|__|____/                |-----------------------|  |  -
 * /// 20h /  32|                       |  |  ^          10h / 16| Field length       *22|  |   |
 * ///          |- - - - - - - - - - - -|  |  |                  |-----------------------|  |   | *7
 * ///          |                    *19|  |  |          11h / 17| Decimal count      *23|  |   |
 * ///          |- - - - - - - - - - - -|  |  Field              |-----------------------|  |  -
 * ///          |                       |  | Descriptor  12h / 18| ( Reserved for        |  |
 * ///          :. . . . . . . . . . . .:  |  |array     13h / 19|   multi-user dBASE)*18|  |
 * ///          :                       :  |  |                  |-----------------------|  |
 * ///       n  |                       |__|__v_         14h / 20| Work area ID       *16|  |
 * ///          |-----------------------|  |    \                |-----------------------|  |
 * ///       n+1| Terminator (0Dh)      |  |     \       15h / 21| ( Reserved for        |  |
 * ///          |=======================|  |      \      16h / 22|   multi-user dBASE )  |  |
 * ///       m  | Database Container    |  |       \             |-----------------------|  |
 * ///          :                    *15:  |        \    17h / 23| Flag for SET FIELDS   |  |
 * ///          :                       :  |         |           |-----------------------|  |
 * ///     / m+263                      |  |         |   18h / 24| ( Reserved )          |  |
 * ///          |=======================|__v_ ___    |           :                       :  |
 * ///          :                       :    ^       |           :                       :  |
 * ///          :                       :    |       |           :                       :  |
 * ///          :                       :    |       |   1Eh / 30|                       |  |
 * ///          | Record structure      |    |       |           |-----------------------|  |
 * ///          |                       |    |        \  1Fh / 31| Index field flag    *8|  |
 * ///          |                       |    |         \_        |=======================| _v_____
 * ///          |                       | Records
 * ///          |-----------------------|    |
 * ///          |                       |    |          _        |=======================| _______
 * ///          |                       |    |         / 00h /  0| Record deleted flag *9|  ^
 * ///          |                       |    |        /          |-----------------------|  |
 * ///          |                       |    |       /           | Data               *10|  One
 * ///          |                       |    |      /            : (ASCII)            *17: record
 * ///          |                       |____|_____/             |                       |  |
 * ///          :                       :    |                   |                       | _v_____
 * ///          :                       :____|_____              |=======================|
 * ///          :                       :    |
 * ///          |                       |    |
 * ///          |                       |    |
 * ///          |                       |    |
 * ///          |                       |    |
 * ///          |                       |    |
 * ///          |=======================|    |
 * ///          |__End_of_File__________| ___v____  End of file ( 1Ah )  *11
 */

public class DbfHeader {
    /**
     * Header file descriptor size is 33 bytes (32 bytes + 1 terminator byte), followed by column metadata which is 32 bytes each.
     */
    public final int fileDescriptorSize = 33;

    /**
     * Field or DBF Column descriptor is 32 bytes long.
     */
    public final int columnDescriptorSize = 32;

    /**
     * type of the file, must be 03h
     */
    private final int fileType = 0x03;

    /**
     * Date the file was last updated.
     */
    private Date updateDate;

    /**
     * Number of records in the datafile, 32bit little-endian, unsigned
     */
    private long numberOfRecords = 0;

    /**
     * Length of the header structure
     * empty header is 33 bytes long. Each column adds 32 bytes.
     */
    private int headerLength = fileDescriptorSize;

    /**
     * Length of the records, ushort - unsigned 16 bit integer
     */
    private int recordLength = 1;  //start with 1 because the first byte is a delete flag

    /**
     * DBF fields/columns
     */
    private List<DbfColumn> fields;

    /**
     * indicates whether header columns can be modified!
     */
    private boolean locked = false;

    /**
     * Keeps column name index for the header, must clear when header columns change.
     */
    private Map<String, Integer> columnNameIndex = null;

    /**
     * When object is modified dirty flag is set.
     */
    private boolean isDirty = false;

    /**
     * emptyRecord is an array used to clear record data in CDbf4Record.
     * This is shared by all record objects, used to speed up clearing fields or entire record.
     * @see #getEmptyDataRecord
     */
    private byte[] emptyRecord = null;
    /**
     * Byte that contains information about the encoding used to write the DBF file. If this byte does not contain
     * valid data, windows-1252 will be used by default. Also, if the encoding is provided externally, e.g. through
     * a CPG file, this byte will be ignored.
     */
    private byte languageDriverId;

    private Configuration configuration;


    public DbfHeader(Configuration configuration) {
        // create a list of fields of default size
        fields = new ArrayList<DbfColumn>();

        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, 1900);
        cal.set(Calendar.MONTH, 1);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        updateDate = cal.getTime();
        this.configuration = configuration;
    }

    /**
     * Create DbfHeader object using the specified column capacity.
     *
     * @param fieldCapacity initial column capacity
     */
    public DbfHeader(int fieldCapacity, Configuration configuration) {
        fields = new ArrayList<DbfColumn>(fieldCapacity);
        this.configuration = configuration;
    }

    /**
     * Gets header length.
     *
     * @return
     */
    public int headerLength() {
        return headerLength;
    }

    /**
     * Add a new column to the DBF header.
     *
     * @param newColumn column to be added
     * @exception IllegalStateException If trying to modify header while it is locked.
     * @exception IllegalArgumentException If the column add would cause record length overflow.
     */
    public void addColumn(DbfColumn newColumn) {
        // throw exception if the header is locked
        if (locked) {
            throw new IllegalStateException("This header is locked and cannot be modified. " +
                    "Modifying the header would result in a corrupt DBF file. " +
                    "You can unlock the header by calling unlock() method.");
        }

        //since we are breaking the spec rules about max number of fields, we should at least
        //check that the record length stays within a number that can be recorded in the header!
        //we have 2 unsigned bytes for record length for a maximum of 65535.
        if (recordLength + newColumn.getLength() > 65535)
            throw new IllegalArgumentException("Unable to add new column. Adding this column puts the record length over the maximum (which is 65535 bytes).");

        // add the column
        fields.add(newColumn);

        // update offset bits, record and header lengths
        newColumn.dataAddress = recordLength;
        recordLength += newColumn.getLength();
        headerLength += columnDescriptorSize;

        // clear empty record
        emptyRecord = null;

        // set dirty bit
        isDirty = true;
        columnNameIndex = null;
    }

    /**
     * Create and add a new column with specified name and type.
     *
     * @param name Name of the new column.
     * @param type Type of the new column.
     */
    public void addColumn(String name, DbfColumn.DbfColumnType type) {
        addColumn(new DbfColumn(name, type));
    }

    /**
     * Create and add a new column with specified name, type, length, and decimal precision.
     *
     * @param name     Field name. Uniqueness is not enforced.
     * @param type     Type of the new column.
     * @param length   Length of the field including decimal point and decimal numbers.
     * @param decimals Number of decimal places to keep.
     */
    public void addColumn(String name, DbfColumn.DbfColumnType type, int length, int decimals) {
        addColumn(new DbfColumn(name, type, length, decimals));
    }

    /**
     * Remove column from header definition.
     *
     * @param index Index of the column to be removed.
     * @exception IllegalStateException If trying to modify header while it is locked.
     */
    public void removeColumn(int index) {
        // throw exception if the header is locked
        if (locked) {
            throw new IllegalStateException("This header is locked and can not be modified. " +
                    "Modifying the header would result in a corrupt DBF file. " +
                    "You can unlock the header by calling UnLock() method.");
        }

        DbfColumn oColRemove = fields.get(index);
        fields.remove(index);


        oColRemove.dataAddress = 0;
        recordLength -= oColRemove.getLength();
        headerLength -= columnDescriptorSize;

        // if you remove a column offset shift for each of the columns
        // following the one removed, we need to update those offsets.
        int nRemovedColLen = oColRemove.getLength();
        for (int i = index; i < fields.size(); i++) {
            fields.get(i).dataAddress -= nRemovedColLen;
        }

        //clear the empty record
        emptyRecord = null;

        //set dirty bit
        isDirty = true;
        columnNameIndex = null;
    }

    /**
     * Look up a column index by name. Note that this is case sensitive.
     */
    public DbfColumn getColumn(String columnName) {
        int colIndex = findColumn(columnName);
        if (colIndex > -1)
            return fields.get(colIndex);

        return null;
    }

    /**
     * Gets column at specified index. Index is 0 based.
     * @param index Zero based index.
     * @return DbfColumn object at specified index.
     */
    public DbfColumn get(int index) {
        return fields.get(index);
    }

    /**
     * Finds a column index by using a fast dictionary lookup-- creates column dictionary on first use. Returns -1 if not found. Note this is case sensitive!
     * @param columnName Column name
     * @return column index (0 based) or -1 if not found
     */
    public int findColumn(String columnName) {
        if (columnNameIndex == null) {
            // Create a new index
            columnNameIndex = new HashMap<String, Integer>(fields.size());
            for (int i = 0; i < fields.size(); i++) {
                columnNameIndex.put(fields.get(i).getName(), i);
            }
        }

        Integer columnIndex = columnNameIndex.get(columnName);
        if(columnIndex != null) {
             return columnIndex;
        }

        return -1;
    }

    /**
     * Returns an empty data record. This is used to clear columns. The reason we put this in the header class
     * is because it allows us to use the CDbf4Record class in two ways.
     * 1. we can create one instance of the record and reuse it to write many records quickly
     *    clearing the data array by bitblting to it.
     * 2. we can create many instances of the record (a collection of records) and have only one copy
     *    of this empty dataset for all of them.
     * If we had put it in the Record class then we would be taking up twice as much space unnecessarily.
     * The empty record also fits the model and everything is neatly encapsulated and safe.
     *
     * @return An empty data record.
     * @throws UnsupportedEncodingException If the named charset is not supported
     */
    protected byte[] getEmptyDataRecord() throws UnsupportedEncodingException {
        if (emptyRecord == null) { // create lazily
            String value = String.format("%1$" + recordLength + "s", " ");
            emptyRecord = value.getBytes(configuration.getEncodingName());
        }

        return emptyRecord;
    }

    /**
     * @return Number of columns in this dbf header.
     */
    public int getColumnCount() {
        return fields.size();
    }

    /**
     * Size of one record in bytes. All fields + 1 byte delete flag.
     * @return
     */
    public int getRecordLength() {
        return recordLength;
    }

    /**
     * @return Number of records in the DBF.
     */
    public long getRecordCount() {
        return numberOfRecords;
    }

    /**
     * The reason client is allowed to set recordCount is because in certain streams
     * like internet streams we can not update record count as we write out records,
     * we have to set it in advance, so client has to be able to modify this property.
     * @param value
     */
    public void setRecordCount(long value) {
        numberOfRecords = value;

        // Set the dirty bit
        isDirty = true;
    }

    /**
     * Get/set whether this header is read only or can be modified. When you create a DbfRecord
     * object and pass a header to it, DbfRecord locks the header so that it can not be modified
     * any longer in order to preserve DBF integrity.
     * @return true if the header is locked, false otherwise
     */
    boolean getLocked() {
        return locked;
    }

    void setLocked(boolean value) {
        locked = value;
    }

    /**
     * Use this method with caution. Headers are locked for a reason, to prevent DBF from becoming corrupt.
     */
    public void unlock() {
        locked = false;
    }

    /**
     * @return true if this object is modified after read or write
     */
    public boolean getIsDirty() {
        return isDirty;
    }

    public void setIsDirty(boolean value) {
        isDirty = value;
    }

    /**
     * See class description for DBF file structure.
     *
     *
     * @param dbfFileWriter DBF file writer.
     * @throws IOException  if an I/O error occurs.
     */
    public void write(FileReader dbfFileWriter) throws IOException {
        // write the header
        // write the output file type.
        dbfFileWriter.writeByte((byte) fileType);

        // Update date format is YYMMDD, which is different from the column Date type (YYYYDDMM)
        SimpleDateFormat sdf = new SimpleDateFormat("yy");
        dbfFileWriter.writeByte((byte) Integer.parseInt(sdf.format(updateDate)));

        sdf.applyPattern("MM");
        dbfFileWriter.writeByte((byte) Integer.parseInt(sdf.format(updateDate)));

        sdf.applyPattern("dd");
        dbfFileWriter.writeByte((byte) Integer.parseInt(sdf.format(updateDate)));

        // write the number of records in the datafile. (32 bit number, little-endian unsigned)
        dbfFileWriter.writeLittleEndianInt((int) numberOfRecords);

        // write the length of the header structure - 2 byte short
        dbfFileWriter.writeLittleEndianShort((short) headerLength);

        // write the length of a record
        dbfFileWriter.writeLittleEndianShort((short) recordLength);

        // write the reserved bytes in the header
        for (int i = 0; i < 20; i++) {
            dbfFileWriter.write((byte) 0);
        }

        // write all of the header records
        byte[] byteReserved = new byte[14];  // these are initialized to 0 by default.
        for (int i = 0; i < fields.size(); i++) {
            int padValue = 11;
            String value = String.format("%1$-" + padValue + "s", "0");
            value += fields.get(i).getName();
            // field name length is up to 10 characters, and is terminated with a NULL character
            char[] fieldNamePadded = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
            char[] fieldNameNoPadding = value.substring(value.length() - fields.get(i).getName().length()).toCharArray();
            System.arraycopy(fieldNameNoPadding, 0, fieldNamePadded, 0, fieldNameNoPadding.length);
            dbfFileWriter.write8bitChars(fieldNamePadded);

            // write the field type
            dbfFileWriter.write8bitChar(fields.get(i).getColumnTypeChar());

            // write the field data address, offset from the start of the record
            dbfFileWriter.writeLittleEndianInt(fields.get(i).getDataAddress());

            // write the length of the field.
            // if char field is longer than 255 bytes, then we use the decimal field as part of the field length.
            if (fields.get(i).getColumnType() == DbfColumn.DbfColumnType.CHARACTER && fields.get(i).getLength() > 255) {
                // treat decimal count as high byte of field length, this extends char field max to 65535
                dbfFileWriter.writeLittleEndianShort((short) fields.get(i).getLength());
            } else {
                // write the length of the field.
                dbfFileWriter.write((byte) fields.get(i).getLength());

                // write the decimal count.
                dbfFileWriter.write((byte) fields.get(i).getDecimalCount());
            }

            // write the reserved bytes.
            dbfFileWriter.write(byteReserved);
        }

        // write the end of the field definitions marker
        dbfFileWriter.write((byte) 0x0D);

        // Clear dirty bit.
        isDirty = false;

        // Lock the header so it cannot be modified any longer.
        locked = true; // We could actually postpone this until first record is written.
    }

    /**
     * Read header data, make sure the stream is positioned at the start of the file to read the header otherwise you will get an exception.
     * When this function is done the position will be the first record.
     *
     * @param dbfFile little endian reader
     * @throws IOException if an I/O error occurs.
     * @throws InvalidDbfFileException If the DBF file is not valid.
     */
    public void read(FileReader dbfFile) throws IOException, InvalidDbfFileException {
        // Type of reader
        int fileType = dbfFile.readByte();
        if (fileType != 0x03) {
            throw new InvalidDbfFileException("Unsupported DBF reader Type " + fileType);
        }

        // Update date
        int year = (int) dbfFile.readByte();
        int month = (int) dbfFile.readByte();
        int day = (int) dbfFile.readByte();
        Calendar cal = Calendar.getInstance();
        cal.set(year + 1900, month, day);
        updateDate = cal.getTime();

        // Number of records
        int numRecordsInt = dbfFile.readLittleEndianInt(); // signed integer
        numberOfRecords = ByteUtils.getUnsigned(numRecordsInt); // unsigned long

        // Length of the header structure
        headerLength = dbfFile.readLittleEndianUnsignedShort();

        // Length of a record
        recordLength = dbfFile.readLittleEndianShort();

        // Skip everything till the language driver ID.
        dbfFile.skipBytes(17);

        // Language driver ID.
        languageDriverId = dbfFile.readByte();
        tryToSetEncoding();

        // Skip two reserved bytes (30. and 31. byte).
        dbfFile.skipBytes(2);

        // Calculate the number of fields in the header
        int nNumFields = (headerLength - fileDescriptorSize) / columnDescriptorSize;

        // Offset from start of record, start at 1 because that's the delete flag
        int dataOffset = 1;

        // Read all of the header records
        fields = new ArrayList<DbfColumn>(nNumFields);
        for (int i = 0; i < nNumFields; i++) {
            /**
             * read the field name.
             * field name: 10 8-bit characters, ASCII (terminated by 00h)
             */
            String sFieldName = dbfFile.readChars(11, configuration.getEncodingName());

            // read the field type
            byte fieldType = dbfFile.readByte();
            char cDbaseType = (char) fieldType;

            // read the field data address, offset from the start of the record.
            int nFieldDataAddress = dbfFile.readLittleEndianInt();

            // read the field length in bytes
            // if field type is char, then read FieldLength and Decimal count as one number to allow char fields to be
            // longer than 256 bytes (ASCII char). This is the way Clipper and FoxPro do it, and there is really no downside
            // since for char fields decimal count should be zero for other versions that do not support this extended functionality.\
            int fieldLength = 0;
            int nDecimals = 0;
            if (cDbaseType == 'C' || cDbaseType == 'c') {
                //treat decimal count as high byte
                fieldLength = dbfFile.readLittleEndianShort();
            } else {
                //read field length as an unsigned byte.
                fieldLength = (int) dbfFile.readByte();

                //read decimal count as one byte
                nDecimals = (int) dbfFile.readByte();
            }

            // read the reserved bytes
            dbfFile.skipBytes(14);

            // create and add field to collection
            fields.add(new DbfColumn(sFieldName, DbfColumn.getDbaseType(cDbaseType), fieldLength, nDecimals, dataOffset));

            // add up address information, you can not trust the address recorded in the DBF file...
            dataOffset += fieldLength;
        }

        // Last byte is a marker for the end of the field definitions.
        dbfFile.skipBytes(1);

        // read any extra header bytes... move to first record
        // equivalent to reader.BaseStream.Seek(headerLength, SeekOrigin.Begin) except that we are not using the seek function since
        // we need to support streams that can not seek like web connections.
        int extraReadBytes = headerLength - (fileDescriptorSize + (columnDescriptorSize * fields.size()));
        if (extraReadBytes > 0) {
            dbfFile.skipBytes(extraReadBytes);
        }

        //if the stream is not forward-only, calculate number of records using file size,
        //sometimes the header does not contain the correct record count
        //if we are reading the file from the web, we have to use readNext() functions anyway so
        //Number of records is not so important and we can trust the DBF to have it stored correctly.
        if (numberOfRecords == 0) {
                //notice here that we subtract file end byte which is supposed to be 0x1A,
                //but some DBF files are incorrectly written without this byte, so we round off to nearest integer.
                //that gives a correct result with or without ending byte.
            if (recordLength > 0) {
                numberOfRecords = Math.round(((double) (dbfFile.length() - headerLength - 1) / recordLength));
            }
        }

        // Lock header since it was read from a file. we don't want it modified because that would corrupt the file.
        // User can override this lock if really necessary by calling #unlock() method.
        locked = true;

        // Clear dirty bit
        isDirty = false;
    }

    /**
     * Try to set encoding using the byte in the language driver ID. Encoding will be set if the byte contains
     * valid encoding data and if the encoding has not been externally force (e.g. through a CPG file). If the
     * language driver ID does not contain valid encoding, default encoding will be used.
     */
    private void tryToSetEncoding() {
        String encoding = DbfEncodings.getCodePageFromLanguageId(languageDriverId);
        if (encoding != null && Charset.isSupported(encoding) && configuration.getShouldTryToSetEncodingFromLanguageDriver()) {
            configuration.setEncodingName(encoding);
        } // leave default value otherwise
    }

    public Configuration getConfiguration() {
        return configuration;
    }
}

