package com.socialexplorer.fastDBF4j;

import java.lang.reflect.Type;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/***
 * This class represents a DBF Column.
 *
 * Note that certain properties can not be modified after creation of the object.
 * This is because we are locking the header object after creation of a data row,
 * and columns are part of the header so either we have to have a lock field for each column,
 * or make it so that certain properties such as length can only be set during creation of a column.
 * Otherwise a user of this object could modify a column that belongs to a locked header and thus corrupt the DBF file.
 */
public class DbfColumn implements Cloneable {
    /*
     (FoxPro/FoxBase) Double integer *NOT* a memo field
     G 	General 	(dBASE V: like Memo) OLE Objects in MS Windows versions 
     P 	Picture 	(FoxPro) Like Memo fields, but not for text processing. 
     Y 	Currency 	(FoxPro)
     T 	DateTime 	(FoxPro)
     I 	Integer 	Length: 4 byte little endian integer 	(FoxPro)
     F 	Float       Number stored as a string, right justified, and padded with blanks to the width of the field.
    */

    //  Great information on DBF located here:
    //  http://www.clicketyclick.dk/databases/xbase/format/data_types.html
    //  http://www.clicketyclick.dk/databases/xbase/format/dbf.html
    public enum DbfColumnType {
        /**
         * Character  less than 254 length
         * ASCII text less than 254 characters long in dBASE.
         * Character fields can be up to 32 KB long (in Clipper and FoxPro) using decimal
         * count as high byte in field length. It's possible to use up to 64KB long fields
         * by reading length as unsigned.
         */
        CHARACTER(0, 'C'),
        /**
         *
         * Number 	Length: less than 18
         * ASCII text up till 18 characters long (include sign and decimal point).
         * Valid characters:
         *      "0" - "9" and "-". Number fields can be up to 20 characters long in FoxPro and Clipper.         *
         * We are not enforcing this 18 char limit.
         */
        NUMBER(1, 'N'),
        /**
         * L  Logical  Length: 1    Boolean/byte (8 bit)
         *
         * Legal values:
         *  	Not initialized (default)
         *  	Y,y 	Yes
         *  	N,n 	No
         *  	F,f 	False
         *  	T,t 	True
         *  	Logical fields are always displayed using T/F/?. Some sources claims
         *  	that space (ASCII 20h) is valid for not initialised. Space may occur, but is not defined.
         */
        BOOLEAN(2, 'L'),
        /**
         * D 	Date 	Length: 8  Date in format YYYYMMDD. A date like 0000-00- 00 is *NOT* valid.
         */
        DATE(3, 'D'),
        /**
         * M 	Memo 	Length: 10 	Pointer to ASCII text field in memo file 10 digits representing a pointer to a DBT block (default is blanks).
         */
        MEMO(4, 'M'),
        /**
         * B 	Binary 	 	(dBASE V) Like Memo fields, but not for text processing.
         */
        BINARY(5, 'B'),
        /**
         * I 	Integer 	Length: 4 byte little endian integer 	(FoxPro)
         */
        INTEGER(6, 'I'),
        /**
         * F 	Float       Number stored as a string, right justified, and padded with blanks to the width of the field.
         */
        FLOAT(7, 'F');

        private int code;
        private char c;

        /**
         * Value that will be written to DBF when setting null value,
         * and that will be read as null when found in DBF.
         */
        private String nullValue = null;

        public void setNullValue(String nullValue) {
            this.nullValue = nullValue;
        }

        public String getNullValue() {
            return this.nullValue;
        }

        public boolean isNullValue(String value) {
            return this.nullValue.equals(value.trim());
        }

        private DbfColumnType(int code, char c) {
            this.code = code;
            this.c = c;
        }

        public int getCode() {
            return code;
        }

        public char getChar() {
            return c;
        }

        private static Map<Character, DbfColumnType> dbfColumnCharMap = new HashMap();
        static {
            for (DbfColumnType dbfColumnType : EnumSet.allOf(DbfColumnType.class)) {
                dbfColumnCharMap.put(dbfColumnType.getChar(), dbfColumnType);
            }
        }

        public static DbfColumnType getTypeFromChar(char c) {
            return dbfColumnCharMap.get(c);
        }
    }

    /***
     * Column (field) name
     */
    private String name;

    /***
     * Field Type (Char, number, boolean, date, memo, binary)
     */
    private DbfColumnType type;

    /***
     * Offset from the start of the record
     */
    int dataAddress;

    /***
     * Length of the data in bytes; some rules apply which are in the spec (read more above).
     */
    private int length;

    /***
     * Decimal precision count, or number of digits after decimal point. This applies to Number types only.
     */
    private int decimalCount;

    /***
     * Full spec constructor sets all relevant fields.
     * @param name
     * @param type
     * @param length
     * @param decimalPlaces
     * @exception  IllegalArgumentException If the given arguments are invalid.
     */
    public DbfColumn(String name, DbfColumnType type, int length, int decimalPlaces) {
        setName(name);
        this.type = type;
        this.length = length;

        if (type == DbfColumnType.NUMBER) {
            decimalCount = decimalPlaces;
        }
        else {
            decimalCount = 0;
        }

        // perform some simple integrity checks...
        //-------------------------------------------

        // decimal precision:
        // we could also fix the length property with a statement like this: mLength = decimalCount + 2;
        if (decimalCount > 0 && this.length - decimalCount <= 1)
            throw new IllegalArgumentException("Decimal precision can not be larger than the length of the field.");

        if (this.type == DbfColumnType.INTEGER)
            this.length = 4;

        if (this.type == DbfColumnType.BINARY)
            this.length = 1;

        if (this.type == DbfColumnType.DATE)
            this.length = 8;  // Dates are exactly yyyyMMdd

        if (this.type == DbfColumnType.MEMO)
            this.length = 10;  // Length: 10 Pointer to ASCII text field in memo file. pointer to a DBT block.

        if (this.type == DbfColumnType.BOOLEAN)
            this.length = 1;

        // Field length check
        if (this.length <= 0)
            throw new IllegalArgumentException("Invalid field length specified. Field length can not be zero or less than zero.");
        else if (type != DbfColumnType.CHARACTER && type != DbfColumnType.BINARY && this.length > 255)
            throw new IllegalArgumentException("Invalid field length specified. For numbers it should be within 20 digits, but we allow up to 255. For Char and binary types, length up to 65,535 is allowed. For maximum compatibility use up to 255.");
        else if ((type == DbfColumnType.CHARACTER || type == DbfColumnType.BINARY) && this.length > 65535)
            throw new IllegalArgumentException("Invalid field length specified. For Char and binary types, length up to 65535 is supported. For maximum compatibility use up to 255.");

    }

    /***
     * Create a new column fully specifying all properties.
     * @param name column name
     * @param type type of field
     * @param length field length including decimal places and decimal point if any
     * @param decimalPlaces decimal places
     * @param dataAddress offset from start of record
     * @throws IllegalArgumentException If this constructor is used to create number or character field types.
     */
    public DbfColumn(String name, DbfColumnType type, int length, int decimalPlaces, int dataAddress) {
        this(name, type, length, decimalPlaces);
        this.dataAddress = dataAddress;
    }

    public DbfColumn(String name, DbfColumnType type) {
        this(name,type,0,0);

        if (type == DbfColumnType.NUMBER || type == DbfColumnType.CHARACTER)
            throw new IllegalArgumentException("For number and character field types you must specify Length and Decimal Precision.");
    }

    /***
     * @return Field name.
     */
    public String getName() {
        return name;
    }

    /**
     * @param value
     * @exception IllegalArgumentException If the field name is null, empty, or larger than 11 character.
     */
    public void setName(String value)  {
        if (value == null || value.equals("")) {
            throw new IllegalArgumentException("Field names must be at least one char long and cannot be null.");
        }

        if (value.length() > 11) {
            throw new IllegalArgumentException("Field names cannot be longer than 11 chars.");
        }

        name = value;
    }

    /**
     * @return Field type (C N L D or M)..
     */
    public DbfColumnType getColumnType() {
        return type;
    }

    /***
     * N=number, C=char, B=binary, L=boolean, D=date, I=integer, M=memo
     * @return column type as a char, (as written in the DBF column header)
     * @throws IllegalStateException If the field type is not set (is null).
     */
    public char getColumnTypeChar() {
        if (type == null) {
            throw new IllegalStateException("Filed type not set.");
        }
        return type.getChar();
    }

    /***
     * Field Data Address offset from the start of the record.
     * @return
     */
    public int getDataAddress() {
        return dataAddress;
    }

    /***
     * Length of the data in bytes.
     * @return
     */
    public int getLength() {
        return length;
    }

    /***
     * Field decimal count in Binary, indicating where the decimal is.
     * @return
     */
    public int getDecimalCount() {
        return decimalCount;
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

        throw new UnsupportedOperationException(String.format(type.getClass() + " does not have a corresponding dbase type."));
    }

    /**
     * Get dbase column type for character.
     * @param c
     * @return Dbf column type
     * @exception IllegalArgumentException If the given character does not have a corresponding dbase type.
     */
    public static DbfColumnType getDbaseType(char c) {
        String value = Character.toString(c).toUpperCase();
        char[] chars = value.toCharArray();
        DbfColumnType dbfColumnType = DbfColumnType.getTypeFromChar(chars[0]);

        if (dbfColumnType == null) {
            throw new IllegalArgumentException(c+" does not have a corresponding dbase type.");
        }

        return dbfColumnType;
    }

    /***
     * Returns shp file Shape Field.
     * @return
     */
    public static DbfColumn getShapeField() {
        return new DbfColumn("Geometry", DbfColumnType.BINARY);
    }

    /***
     * Returns Shp file ID field.
     * @return
     */
    public static DbfColumn getIdField() {
        return new DbfColumn("Row", DbfColumnType.INTEGER);
    }
}