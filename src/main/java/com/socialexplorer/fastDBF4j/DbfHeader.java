package com.socialexplorer.fastDBF4j;

import com.socialexplorer.util.ByteUtils;
import com.socialexplorer.util.RandomAccessFileWithLittleEndian;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

/***
 /// This class represents a DBF IV file header.
 ///
 /// DBF files are really wasteful on space but this legacy format lives on because it's really really simple.
 /// It lacks much in features though.
 ///
 ///
 /// Thanks to Erik Bachmann for providing the DBF file structure information!!
 /// http://www.clicketyclick.dk/databases/xbase/format/dbf.html
 ///
 ///           _______________________  _______
 /// 00h /   0| Version number      *1|  ^
 ///          |-----------------------|  |
 /// 01h /   1| Date of last update   |  |
 /// 02h /   2|      YYMMDD        *21|  |
 /// 03h /   3|                    *14|  |
 ///          |-----------------------|  |
 /// 04h /   4| Number of records     | Record
 /// 05h /   5| in data file          | header
 /// 06h /   6| ( 32 bits )        *14|  |
 /// 07h /   7|                       |  |
 ///          |-----------------------|  |
 /// 08h /   8| Length of header   *14|  |
 /// 09h /   9| structure ( 16 bits ) |  |
 ///          |-----------------------|  |
 /// 0Ah /  10| Length of each record |  |
 /// 0Bh /  11| ( 16 bits )     *2 *14|  |
 ///          |-----------------------|  |
 /// 0Ch /  12| ( Reserved )        *3|  |
 /// 0Dh /  13|                       |  |
 ///          |-----------------------|  |
 /// 0Eh /  14| Incomplete transac.*12|  |
 ///          |-----------------------|  |
 /// 0Fh /  15| Encryption flag    *13|  |
 ///          |-----------------------|  |
 /// 10h /  16| Free record thread    |  |
 /// 11h /  17| (reserved for LAN     |  |
 /// 12h /  18|  only )               |  |
 /// 13h /  19|                       |  |
 ///          |-----------------------|  |
 /// 14h /  20| ( Reserved for        |  |            _        |=======================| ______
 ///          |   multi-user dBASE )  |  |           / 00h /  0| Field name in ASCII   |  ^
 ///          : ( dBASE III+ - )      :  |          /          : (terminated by 00h)   :  |
 ///          :                       :  |         |           |                       |  |
 /// 1Bh /  27|                       |  |         |   0Ah / 10|                       |  |
 ///          |-----------------------|  |         |           |-----------------------| For
 /// 1Ch /  28| MDX flag (dBASE IV)*14|  |         |   0Bh / 11| Field type (ASCII) *20| each
 ///          |-----------------------|  |         |           |-----------------------| field
 /// 1Dh /  29| Language driver     *5|  |        /    0Ch / 12| Field data address    |  |
 ///          |-----------------------|  |       /             |                     *6|  |
 /// 1Eh /  30| ( Reserved )          |  |      /              | (in memory !!!)       |  |
 /// 1Fh /  31|                     *3|  |     /       0Fh / 15| (dBASE III+)          |  |
 ///          |=======================|__|____/                |-----------------------|  |  -
 /// 20h /  32|                       |  |  ^          10h / 16| Field length       *22|  |   |
 ///          |- - - - - - - - - - - -|  |  |                  |-----------------------|  |   | *7
 ///          |                    *19|  |  |          11h / 17| Decimal count      *23|  |   |
 ///          |- - - - - - - - - - - -|  |  Field              |-----------------------|  |  -
 ///          |                       |  | Descriptor  12h / 18| ( Reserved for        |  |
 ///          :. . . . . . . . . . . .:  |  |array     13h / 19|   multi-user dBASE)*18|  |
 ///          :                       :  |  |                  |-----------------------|  |
 ///       n  |                       |__|__v_         14h / 20| Work area ID       *16|  |
 ///          |-----------------------|  |    \                |-----------------------|  |
 ///       n+1| Terminator (0Dh)      |  |     \       15h / 21| ( Reserved for        |  |
 ///          |=======================|  |      \      16h / 22|   multi-user dBASE )  |  |
 ///       m  | Database Container    |  |       \             |-----------------------|  |
 ///          :                    *15:  |        \    17h / 23| Flag for SET FIELDS   |  |
 ///          :                       :  |         |           |-----------------------|  |
 ///     / m+263                      |  |         |   18h / 24| ( Reserved )          |  |
 ///          |=======================|__v_ ___    |           :                       :  |
 ///          :                       :    ^       |           :                       :  |
 ///          :                       :    |       |           :                       :  |
 ///          :                       :    |       |   1Eh / 30|                       |  |
 ///          | Record structure      |    |       |           |-----------------------|  |
 ///          |                       |    |        \  1Fh / 31| Index field flag    *8|  |
 ///          |                       |    |         \_        |=======================| _v_____
 ///          |                       | Records
 ///          |-----------------------|    |
 ///          |                       |    |          _        |=======================| _______
 ///          |                       |    |         / 00h /  0| Record deleted flag *9|  ^
 ///          |                       |    |        /          |-----------------------|  |
 ///          |                       |    |       /           | Data               *10|  One
 ///          |                       |    |      /            : (ASCII)            *17: record
 ///          |                       |____|_____/             |                       |  |
 ///          :                       :    |                   |                       | _v_____
 ///          :                       :____|_____              |=======================|
 ///          :                       :    |
 ///          |                       |    |
 ///          |                       |    |
 ///          |                       |    |
 ///          |                       |    |
 ///          |                       |    |
 ///          |=======================|    |
 ///          |__End_of_File__________| ___v____  End of file ( 1Ah )  *11
 */

public class DbfHeader implements Cloneable {
    /***
     * Header file descriptor size is 33 bytes (32 bytes + 1 terminator byte), followed by column metadata which is 32 bytes each.
     */
    public final int fileDescriptorSize = 33;

    /***
     * Field or DBF Column descriptor is 32 bytes long.
     */
    public final int columnDescriptorSize = 32;

    /***
     * type of the file, must be 03h
     */
    private final int _fileType = 0x03;

    /***
     * Date the file was last updated.
     */
    private Date _updateDate;

    /***
     * Number of records in the datafile, 32bit little-endian, unsigned
     */
    private long _numRecords = 0;

    /***
     * Length of the header structure
     * empty header is 33 bytes long. Each column adds 32 bytes.
     */
    private int _headerLength = fileDescriptorSize;

    /***
     * Length of the records, ushort - unsigned 16 bit integer
     */
    private int _recordLength = 1;  //start with 1 because the first byte is a delete flag

    /***
     * DBF fields/columns
     */
    private List<DbfColumn> _fields;

    /***
     * indicates whether header columns can be modified!
     */
    private boolean _locked = false;

    /***
     * keeps column name index for the header, must clear when header columns change.
     */
    private Map<String, Integer> _columnNameIndex = null;

    /***
     * When object is modified dirty flag is set.
     */
    private boolean _isDirty = false;

    /***
     * _emptyRecord is an array used to clear record data in CDbf4Record.
     * This is shared by all record objects, used to speed up clearing fields or entire record.
     * <seealso cref="getEmptyDataRecord"/>
     */
    private byte[] _emptyRecord = null;

    public DbfHeader() {
        // create a list of fields of default size
        _fields = new ArrayList<DbfColumn>();

        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, 1900);
        cal.set(Calendar.MONTH, 1);
        cal.set(Calendar.DAY_OF_MONTH, 1);
        _updateDate = cal.getTime();
    }

    /***
     * Create DbfHeader object using the specified column capacity.
     * @param fieldCapacity initial column capacity
     */
    public DbfHeader(int fieldCapacity) {
        _fields = new ArrayList<DbfColumn>(fieldCapacity);
    }

    /***
     * Gets header length.
     * @return
     */
    public int HeaderLength() {
        return _headerLength;
    }

    /***
     * Add a new column to the DBF header.
     * @param newColumn column to be added
     * @throws Exception
     */
    public void addColumn(DbfColumn newColumn) throws Exception {
        // throw exception if the header is locked
        if (_locked) {
            throw new Exception("This header is locked and cannot be modified. Modifying the header would result in a corrupt DBF file. You can unlock the header by calling unlock() method.");
        }

        //since we are breaking the spec rules about max number of fields, we should at least
        //check that the record length stays within a number that can be recorded in the header!
        //we have 2 unsigned bytes for record length for a maximum of 65535.
        if (_recordLength + newColumn.getLength() > 65535)
            throw new Exception("Unable to add new column. Adding this column puts the record length over the maximum (which is 65535 bytes).");

        // add the column
        _fields.add(newColumn);

        // update offset bits, record and header lengths
        newColumn._dataAddress = _recordLength;
        _recordLength += newColumn.getLength();
        _headerLength += columnDescriptorSize;

        // clear empty record
        _emptyRecord = null;

        // set dirty bit
        _isDirty = true;
        _columnNameIndex = null;
    }

    /***
     * Create and add a new column with specified name and type.
     * @param name Name of the new column.
     * @param type Type of the new column.
     * @throws Exception
     */
    public void addColumn(String name, DbfColumn.DbfColumnType type) throws Exception {
        addColumn(new DbfColumn(name, type));
    }

    /***
     * Create and add a new column with specified name, type, length, and decimal precision.
     * @param name Field name. Uniqueness is not enforced.
     * @param type Type of the new column.
     * @param length Length of the field including decimal point and decimal numbers.
     * @param decimals Number of decimal places to keep.
     * @throws Exception
     */
    public void addColumn(String name, DbfColumn.DbfColumnType type, int length, int decimals) throws Exception {
        addColumn(new DbfColumn(name, type, length, decimals));
    }

    /***
     * Remove column from header definition.
     * @param index Index of the column to be removed.
     * @throws Exception
     */
    public void removeColumn(int index) throws Exception {
        // throw exception if the header is locked
        if (_locked) {
            throw new Exception("This header is locked and can not be modified. Modifying the header would result in a corrupt DBF file. You can unlock the header by calling UnLock() method.");
        }

        DbfColumn oColRemove = _fields.get(index);
        _fields.remove(index);


        oColRemove._dataAddress = 0;
        _recordLength -= oColRemove.getLength();
        _headerLength -= columnDescriptorSize;

        // if you remove a column offset shift for each of the columns
        // following the one removed, we need to update those offsets.
        int nRemovedColLen = oColRemove.getLength();
        for (int i = index; i < _fields.size(); i++) {
            _fields.get(i)._dataAddress -= nRemovedColLen;
        }

        //clear the empty record
        _emptyRecord = null;

        //set dirty bit
        _isDirty = true;
        _columnNameIndex = null;
    }

    /// <summary>
    /// Look up a column index by name. Note that this is case sensitive, internally it does a lookup using a dictionary.
    /// </summary>
    /// <param name="sName"></param>
    /*public com.socialexplorer.geoservices.fastDbf.DbfColumn this[String sName]
    {
        get
        {
            int colIndex = FindColumn(sName);
            if (colIndex > -1)
                return _fields[colIndex];

            return null;
        }
    }*/

    /*public com.socialexplorer.geoservices.fastDbf.DbfColumn get(String sName)
    {
        int colIndex = FindColumn(sName);
        if (colIndex > -1)
            return _fields.get(colIndex);

        return null;
    }    */

    /***
     * Gets column at specified index. Index is 0 based.
     * @param index Zero based index.
     * @return DbfColumn object at specified index.
     */
    public DbfColumn get(int index)
    {
        return _fields.get(index);
    }

    /// <summary>
    /// Finds a column index by using a fast dictionary lookup-- creates column dictionary on first use. Returns -1 if not found. Note this is case sensitive!
    /// </summary>
    /// <param name="sName">Column name</param>
    /// <returns>column index (0 based) or -1 if not found.</returns>
    /*public int FindColumn(String sName) {

        if (_columnNameIndex == null) {
            _columnNameIndex = new HashMap<String, Integer>(_fields.size()); // Dictionary<String, Integer>(_fields.size());

            //create a new index
            for (int i = 0; i < _fields.size(); i++) {
                _columnNameIndex.put(_fields.get(i).getName(), i);
            }
        }

        int columnIndex;
        if(_columnNameIndex.get(sName) != null)
        {

        }
        if (_columnNameIndex.TryGetValue(sName, out columnIndex))
            return columnIndex;

        return -1;
    }  */

    /***
     * Returns an empty data record. This is used to clear columns
     *
     * The reason we put this in the header class is because it allows us to use the CDbf4Record class
     * in two ways.
     * 1. we can create one instance of the record and reuse it to write many records quickly
     * clearing the data array by bitblting to it.
     * 2. we can create many instances of the record (a collection of records) and have only one copy
     * of this empty dataset for all of them.
     * If we had put it in the Record class then we would be taking up twice as much space unnecessarily.
     * The empty record also fits the model and everything is neatly encapsulated and safe.
     * @return an empty data record
     * @throws UnsupportedEncodingException
     */
    protected byte[] getEmptyDataRecord() throws UnsupportedEncodingException
    {
        if (_emptyRecord == null) {
            //initialize array for clearing data quickly
            String value = String.format("%1$" + (int) _recordLength + "s", " ");
            _emptyRecord = value.getBytes("US-ASCII");

            //_emptyRecord = Encoding.ASCII.GetBytes("".PadLeft((int) _recordLength, ' ').ToCharArray());
        }

        return _emptyRecord;
    }

    /***
     * @return Number of columns in this dbf header.
     */
    public int getColumnCount()
    {
        return _fields.size();
    }

    /***
     * Size of one record in bytes. All fields + 1 byte delete flag.
     * @return
     */
    public int getRecordLength()
    {
        return _recordLength;
    }

    /***
     * Get number of records in the DBF.
     * @return
     */
    public long getRecordCount()
    {
        return _numRecords;
    }

    /***
     * The reason we allow client to set RecordCount is because in certain streams
     * like internet streams we can not update record count as we write out records,
     * we have to set it in advance, so client has to be able to modify this property.
     * @param value
     */
    public void setRecordCount(long value)
    {
        _numRecords = value;

        //set the dirty bit
        _isDirty = true;
    }

    /***
     * Get/set whether this header is read only or can be modified. When you create a DbfRecord
     * object and pass a header to it, CDbfRecord locks the header so that it can not be modified
     * any longer in order to preserve DBF integrity.
     * @return
     */
    boolean getLocked()
    {
        return _locked;
    }

    void setLocked(boolean value)
    {
        _locked = value;
    }

    /***
     * Use this method with caution. Headers are locked for a reason, to prevent DBF from becoming corrupt.
     */
    public void unlock() {
        _locked = false;
    }

    /***
     * Returns true when this object is modified after read or write.
     * @return
     */
    public boolean getIsDirty()
    {
        return _isDirty;
    }

    public void setIsDirty(boolean value)
    {
        _isDirty = value;
    }

    /***
     * Encoding must be ASCII for this binary writer.
     * See class description for DBF file structure.
     * @param dbfFile
     * @throws Exception
     */
    public void write(RandomAccessFileWithLittleEndian dbfFile) throws Exception
    {

        // write the header
        // write the output file type.
        dbfFile.writeByte((byte) _fileType);

        // Update date format is YYMMDD, which is different from the column Date type (YYYYDDMM)
        SimpleDateFormat sdf = new SimpleDateFormat("yy");
        dbfFile.writeByte((byte) Integer.parseInt(sdf.format(_updateDate)));

        sdf.applyPattern("MM");
        dbfFile.writeByte((byte) Integer.parseInt(sdf.format(_updateDate)));

        sdf.applyPattern("dd");
        dbfFile.writeByte((byte) Integer.parseInt(sdf.format(_updateDate)));

        // write the number of records in the datafile. (32 bit number, little-endian unsigned)
        dbfFile.writeLittleEndianInt((int) _numRecords);

        // write the length of the header structure - 2 byte short
        dbfFile.writeLittleEndianShort((short) _headerLength);

        // write the length of a record
        dbfFile.writeLittleEndianShort((short) _recordLength);

        // write the reserved bytes in the header
        for (int i = 0; i < 20; i++) {
            dbfFile.write((byte) 0);
        }

        // write all of the header records
        byte[] byteReserved = new byte[14];  // these are initialized to 0 by default.
        for (int i = 0; i < _fields.size(); i++) {
            int padValue = 11;
            String value = String.format("%1$-" + padValue + "s", "0");
            value += _fields.get(i).getName();
            // field name length is up to 10 characters, and is terminated with a NULL character
            char[] fieldNamePadded = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
            char[] fieldNameNoPadding = value.substring(value.length() - _fields.get(i).getName().length()).toCharArray();
            System.arraycopy(fieldNameNoPadding, 0, fieldNamePadded, 0, fieldNameNoPadding.length);
            dbfFile.write8bitChars(fieldNamePadded);

            // write the field type
            dbfFile.write8bitChar((char) _fields.get(i).getColumnTypeChar());

            // write the field data address, offset from the start of the record
            dbfFile.writeLittleEndianInt(_fields.get(i).getDataAddress());

            // write the length of the field.
            // if char field is longer than 255 bytes, then we use the decimal field as part of the field length.
            if (_fields.get(i).getColumnType() == DbfColumn.DbfColumnType.CHARACTER && _fields.get(i).getLength() > 255) {
                // treat decimal count as high byte of field length, this extends char field max to 65535
                dbfFile.writeLittleEndianShort((short) _fields.get(i).getLength());
            } else {
                // write the length of the field.
                dbfFile.write((byte) _fields.get(i).getLength());

                // write the decimal count.
                dbfFile.write((byte) _fields.get(i).getDecimalCount());
            }

            // write the reserved bytes.
            dbfFile.write(byteReserved);
        }

        // write the end of the field definitions marker
        dbfFile.write((byte) 0x0D);

        // clear dirty bit
        _isDirty = false;

        // lock the header so it cannot be modified any longer,
        // we could actually postpone this until first record is written!
        _locked = true;
    }

    /***
     * Read header data, make sure the stream is positioned at the start of the file to read the header otherwise you will get an exception.
     * When this function is done the position will be the first record.
     * @param dbfFile little endian reader
     * @throws IOException
     * @throws Exception
     */
    public void read(RandomAccessFileWithLittleEndian dbfFile) throws IOException, Exception {
        try {
            // type of reader.
            int nFileType = dbfFile.readByte();

            if (nFileType != 0x03) {
                throw new Exception("Unsupported DBF reader Type " + nFileType);
            }

            // parse the update date information
            int year = (int) dbfFile.readByte();
            int month = (int) dbfFile.readByte();
            int day = (int) dbfFile.readByte();
            Calendar cal = Calendar.getInstance();
            cal.set(year + 1900, month, day);
            _updateDate = cal.getTime();

            // read the number of records
            int numRecordsInt = dbfFile.readLittleEndianInt(); // signed integer
            _numRecords = ByteUtils.getUnsigned(numRecordsInt); // unsigned long

            // read the length of the header structure.
            _headerLength = dbfFile.readLittleEndianShort();               // TODO

            // read the length of a record
            _recordLength = dbfFile.readLittleEndianShort();

            // skip the reserved bytes in the header.
            dbfFile.skipBytes(20);

            // calculate the number of fields in the header
            int nNumFields = (_headerLength - fileDescriptorSize) / columnDescriptorSize;

            // offset from start of record, start at 1 because that's the delete flag.
            int nDataOffset = 1;

            // read all of the header records
            _fields = new ArrayList<DbfColumn>(nNumFields);
            for (int i = 0; i < nNumFields; i++) {
                /**
                 * read the field name.
                 * field name: 10 8-bit characters, ASCII (terminated by 00h)
                 */
                String sFieldName = dbfFile.readChars(11);

                // read the field type
                byte fieldType = dbfFile.readByte();
                char cDbaseType = (char) fieldType;

                // read the field data address, offset from the start of the record.
                int nFieldDataAddress = dbfFile.readLittleEndianInt();

                // read the field length in bytes
                // if field type is char, then read FieldLength and Decimal count as one number to allow char fields to be
                // longer than 256 bytes (ASCII char). This is the way Clipper and FoxPro do it, and there is really no downside
                // since for char fields decimal count should be zero for other versions that do not support this extended functionality.\
                int nFieldLength = 0;
                int nDecimals = 0;
                if (cDbaseType == 'C' || cDbaseType == 'c') {
                    //treat decimal count as high byte
                    nFieldLength = (int) dbfFile.readLittleEndianShort();
                } else {
                    //read field length as an unsigned byte.
                    nFieldLength = (int) dbfFile.readByte();

                    //read decimal count as one byte
                    nDecimals = (int) dbfFile.readByte();
                }

                // read the reserved bytes
                dbfFile.skipBytes(14);

                // create and add field to collection
                _fields.add(new DbfColumn(sFieldName, DbfColumn.getDbaseType(cDbaseType), nFieldLength, nDecimals, nDataOffset));

                // add up address information, you can not trust the address recorded in the DBF file...
                nDataOffset += nFieldLength;
            }

            // Last byte is a marker for the end of the field definitions.
            dbfFile.skipBytes(1);

            // read any extra header bytes... move to first record
            // equivalent to reader.BaseStream.Seek(_headerLength, SeekOrigin.Begin) except that we are not using the seek function since
            // we need to support streams that can not seek like web connections.
            int nExtraReadBytes = _headerLength - (fileDescriptorSize + (columnDescriptorSize * _fields.size()));
            if (nExtraReadBytes > 0) {
                dbfFile.skipBytes(nExtraReadBytes);
            }

            /* if the stream is not forward-only, calculate number of records using file size,
                sometimes the header does not contain the correct record count
                if we are reading the file from the web, we have to use readNext() functions anyway so
                Number of records is not so important and we can trust the DBF to have it stored correctly. */
            if (_numRecords == 0) {
//                //notice here that we subtract file end byte which is supposed to be 0x1A,
//                //but some DBF files are incorrectly written without this byte, so we round off to nearest integer.
//                //that gives a correct result with or without ending byte.
                if (_recordLength > 0) {
                    _numRecords = (long) Math.round(((double) (dbfFile.length() - _headerLength - 1) / _recordLength));
                }
            }

            // lock header since it was read from a file. we don't want it modified because that would corrupt the file.
            // user can override this lock if really necessary by calling UnLock() method.
            _locked = true;

            // clear dirty bit
            _isDirty = false;
        } catch (Exception e) {
            throw e;
        }
    }

    public Object clone() throws CloneNotSupportedException {
        return this.clone();
    }
}

