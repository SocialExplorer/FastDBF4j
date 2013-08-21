package com.socialexplorer.fastDBF4j;

import java.lang.reflect.Type;
import java.util.Date;

/***
 * This class represents a DBF Column.
 *
 /// Note that certain properties can not be modified after creation of the object.
 /// This is because we are locking the header object after creation of a data row,
 /// and columns are part of the header so either we have to have a lock field for each column,
 /// or make it so that certain properties such as length can only be set during creation of a column.
 /// Otherwise a user of this object could modify a column that belongs to a locked header and thus corrupt the DBF file.

 */
public class DbfColumn implements Cloneable {
    
    /*
     (FoxPro/FoxBase) Double integer *NOT* a memo field
     G 	General 	(dBASE V: like Memo) OLE Objects in MS Windows versions 
     P 	Picture 	(FoxPro) Like Memo fields, but not for text processing. 
     Y 	Currency 	(FoxPro)
     T 	DateTime 	(FoxPro)
     I 	Integer 	Length: 4 byte little endian integer 	(FoxPro)
    */

///  Great information on DBF located here:
///  http://www.clicketyclick.dk/databases/xbase/format/data_types.html
///  http://www.clicketyclick.dk/databases/xbase/format/dbf.html
    public enum DbfColumnType {

        /// <summary>
        /// Character  less than 254 length
        /// ASCII text less than 254 characters long in dBASE.
        ///
        /// Character fields can be up to 32 KB long (in Clipper and FoxPro) using decimal
        /// count as high byte in field length. It's possible to use up to 64KB long fields
        /// by reading length as unsigned.
        ///
        /// </summary>
        CHARACTER(0),

        /// <summary>
        /// Number 	Length: less than 18
        ///   ASCII text up till 18 characters long (include sign and decimal point).
        ///
        /// Valid characters:
        ///    "0" - "9" and "-". Number fields can be up to 20 characters long in FoxPro and Clipper.
        /// </summary>
        /// <remarks>
        /// We are not enforcing this 18 char limit.
        /// </remarks>
        NUMBER(1),

        /// <summary>
        ///  L  Logical  Length: 1    Boolean/byte (8 bit)
        ///
        ///  Legal values:
        ///   ? 	Not initialised (default)
        ///   Y,y 	Yes
        ///   N,n 	No
        ///   F,f 	False
        ///   T,t 	True
        ///   Logical fields are always displayed using T/F/?. Some sources claims
        ///   that space (ASCII 20h) is valid for not initialised. Space may occur, but is not defined.
        /// </summary>

        BOOLEAN(2),

        /// <summary>
        /// D 	Date 	Length: 8  Date in format YYYYMMDD. A date like 0000-00- 00 is *NOT* valid.
        /// </summary>
        DATE(3),

        /// <summary>
        /// M 	Memo 	Length: 10 	Pointer to ASCII text field in memo file 10 digits representing a pointer to a DBT block (default is blanks).
        /// </summary>
        MEMO(4),

        /// <summary>
        /// B 	Binary 	 	(dBASE V) Like Memo fields, but not for text processing.
        /// </summary>
        BINARY(5),

        /// <summary>
        /// I 	Integer 	Length: 4 byte little endian integer 	(FoxPro)
        /// </summary>
        INTEGER(6);

        private int _code;

        private DbfColumnType(int c) {
            _code = c;
        }

        public int getCode() {
            return _code;
        }
    }

    /***
     * Column (field) name
     */
    private String _name;

    /***
     * Field Type (Char, number, boolean, date, memo, binary)
     */
    private DbfColumnType _type;

    /***
     * Offset from the start of the record
     */
    int _dataAddress;

    /***
     * Length of the data in bytes; some rules apply which are in the spec (read more above).
     */
    private int _length;

    /***
     * Decimal precision count, or number of digits after decimal point. This applies to Number types only.
     */
    private int _decimalCount;

    /***
     * Full spec constructor sets all relevant fields.
     * @param name
     * @param type
     * @param length
     * @param decimalPlaces
     * @throws Exception
     */
    public DbfColumn(String name, DbfColumnType type, int length, int decimalPlaces) throws Exception {
        setName(name);
        _type = type;
        _length = length;

        if (type == DbfColumnType.NUMBER) {
            _decimalCount = decimalPlaces;
        }
        else {
            _decimalCount = 0;
        }

        // perform some simple integrity checks...
        //-------------------------------------------

        // decimal precision:
        // we could also fix the length property with a statement like this: mLength = _decimalCount + 2;
        if (_decimalCount > 0 && _length - _decimalCount <= 1)
            throw new Exception("Decimal precision can not be larger than the length of the field.");

        if (_type == DbfColumnType.INTEGER)
            _length = 4;

        if (_type == DbfColumnType.BINARY)
            _length = 1;

        if (_type == DbfColumnType.DATE)
            _length = 8;  //Dates are exactly yyyyMMdd

        if (_type == DbfColumnType.MEMO)
            _length = 10;  //Length: 10 Pointer to ASCII text field in memo file. pointer to a DBT block.

        if (_type == DbfColumnType.BOOLEAN)
            _length = 1;

        //field length:
        if (_length <= 0)
            throw new Exception("Invalid field length specified. Field length can not be zero or less than zero.");
        else if (type != DbfColumnType.CHARACTER && type != DbfColumnType.BINARY && _length > 255)
            throw new Exception("Invalid field length specified. For numbers it should be within 20 digits, but we allow up to 255. For Char and binary types, length up to 65,535 is allowed. For maximum compatibility use up to 255.");
        else if ((type == DbfColumnType.CHARACTER || type == DbfColumnType.BINARY) && _length > 65535)
            throw new Exception("Invalid field length specified. For Char and binary types, length up to 65535 is supported. For maximum compatibility use up to 255.");

    }

    /***
     * Create a new column fully specifying all properties.
     * @param name column name
     * @param type type of field
     * @param length field length including decimal places and decimal point if any
     * @param decimalPlaces decimal places
     * @param dataAddress offset from start of record
     * @throws Exception
     */
    DbfColumn(String name, DbfColumnType type, int length, int decimalPlaces, int dataAddress)  throws Exception {
        this(name, type, length, decimalPlaces);
        _dataAddress = dataAddress;
    }

    public DbfColumn(String name, DbfColumnType type) throws Exception {
        this(name,type,0,0);

        if (type == DbfColumnType.NUMBER || type == DbfColumnType.CHARACTER)
            throw new Exception("For number and character field types you must specify Length and Decimal Precision.");
    }

    /***
     * Field name.
     * @return
     */
    public String getName() {
        return _name;
    }

    public void setName(String value) throws Exception {
        if (value == null || value == "") {
            throw new Exception("Field names must be at least one char long and can not be null.");
        }

        if (value.length() > 11) {
            throw new Exception("Field names can not be longer than 11 chars.");
        }

        _name = value;
    }

    /***
     * Field Type (C N L D or M).
     * @return
     */
    public DbfColumnType getColumnType() {
        return _type;
    }

    /***
     * Returns column type as a char, (as written in the DBF column header)
     * N=number, C=char, B=binary, L=boolean, D=date, I=integer, M=memo
     * @return
     * @throws Exception
     */
    public char getColumnTypeChar() throws Exception {
        switch (_type) {
            case NUMBER:
                return 'N';
            case CHARACTER:
                return 'C';
            case BINARY:
                return 'B';
            case BOOLEAN:
                return 'L';
            case DATE:
                return 'D';
            case INTEGER:
                return 'I';
            case MEMO:
                return 'M';
        }

        throw new Exception("Unrecognized field type!");
    }

    /***
     * Field Data Address offset from the start of the record.
     * @return
     */
    public int getDataAddress() {
        return _dataAddress;
    }

    /***
     * Length of the data in bytes.
     * @return
     */
    public int getLength() {
        return _length;
    }

    /***
     * Field decimal count in Binary, indicating where the decimal is.
     * @return
     */
    public int getDecimalCount() {
        return _decimalCount;
    }

    /***
     * Returns corresponding dbf field type given a Type.
     * @param type
     * @return
     */
    public static DbfColumnType getDbaseType(Type type) {
        if (type.getClass() == String.class.getClass())
            return DbfColumnType.CHARACTER;
        else if (type.getClass() == double.class.getClass()||type.getClass() == float.class.getClass())
            return DbfColumnType.NUMBER;
        else if (type.getClass() == boolean.class.getClass())
            return DbfColumnType.BOOLEAN;
        else if (type.getClass() == Date.class.getClass())
            return DbfColumnType.DATE;

        throw new UnsupportedOperationException(String.format("{0} does not have a corresponding dbase type.", type.getClass()));
    }

    public static DbfColumnType getDbaseType(char c) throws Exception {
        String value = Character.toString(c).toUpperCase();
        char[] chars = value.toCharArray();
        switch (chars[0]) {
            case 'C':
                return DbfColumnType.CHARACTER;
            case 'N':
                return DbfColumnType.NUMBER;
            case 'B':
                return DbfColumnType.BINARY;
            case 'L':
                return DbfColumnType.BOOLEAN;
            case 'D':
                return DbfColumnType.DATE;
            case 'I':
                return DbfColumnType.INTEGER;
            case 'M':
                return DbfColumnType.MEMO;
        }

        throw new Exception(c+" does not have a corresponding dbase type.");
    }

    /***
     * Returns shp file Shape Field.
     * @return
     * @throws Exception
     */
    public static DbfColumn getShapeField() throws Exception {
        return new DbfColumn("Geometry", DbfColumnType.BINARY);
    }

    /***
     * Returns Shp file ID field.
     * @return
     * @throws Exception
     */
    public static DbfColumn getIdField() throws Exception {
        return new DbfColumn("Row", DbfColumnType.INTEGER);
    }
}