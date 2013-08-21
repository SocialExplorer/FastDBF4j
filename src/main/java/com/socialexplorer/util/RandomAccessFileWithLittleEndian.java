package com.socialexplorer.util;

import java.io.*;

public class RandomAccessFileWithLittleEndian extends RandomAccessFile {
    public RandomAccessFileWithLittleEndian() throws Exception {
        super(new File(""), "");
        throw new Exception("Constructor without parameters not defined!");
    }

    public RandomAccessFileWithLittleEndian(String name, String mode) throws FileNotFoundException {
        super(name, mode);
    }

    public RandomAccessFileWithLittleEndian(File file, String mode) throws FileNotFoundException {
        super(file, mode);
    }

    /***
     * Reads a signed 32-bit little endian integer from this file.
     * Java works with big endian values by default. This method reads a 4-byte little endian signed integer
     * from a file and converts it to big endian.
     *
     * @return the next four bytes of this file, interpreted as an little endian int.
     * @throws IOException
     */
    public int readLittleEndianInt() throws IOException {
        int littleEndianInt = readInt();

        byte[] bytes = ByteUtils.int2byte(littleEndianInt);
        ByteUtils.reverseBytes(bytes);

        return ByteUtils.byte2int(bytes); // big endian int
    }

    /***
     * Writes an int to the file as four bytes, low byte first (little endian byte order).
     * @param bigEndianInt an int to be written
     * @throws IOException if an I/O error occurs.
     */
    public void writeLittleEndianInt(int bigEndianInt) throws IOException {
        byte[] bytes = ByteUtils.int2byte(bigEndianInt);
        ByteUtils.reverseBytes(bytes); // big to little endian byte order
        write(bytes);
    }

    /***
     *
     * @return the next two bytes of this file, interpreted as a little endian 16-bit integer
     */
    public int readLittleEndianShort() throws IOException {
        byte[] shortBytes = new byte[2];
        shortBytes[0] = readByte();
        shortBytes[1] = readByte();
        ByteUtils.reverseBytes(shortBytes); // reverse to interpret as little endian

        return ByteUtils.byte2short(shortBytes); // big endian int
    }

    /***
     * Write two bytes to a file in little endian byte order.
     */
    public void writeLittleEndianShort(short bigEndianShort) throws IOException {
        byte[] shortBytes = ByteUtils.short2byte(bigEndianShort);
        ByteUtils.reverseBytes(shortBytes);
        write(shortBytes);
    }

    /***
     * Read numberOfChars 8-bit characters and create a string out of them. The string is terminated
     * with the NULL character.
     * @param numberOfChars
     * @return
     * @throws IOException
     */
    public String readChars(int numberOfChars) throws IOException {
        byte[] fieldNameBuffer = new byte[11];
        for (int j = 0; j <= 10; j++) {
            fieldNameBuffer[j] = readByte();
        }
        String sFieldName = new String(fieldNameBuffer);
        // remove the terminating NULL character (00h, or 0 decimal)
        int nullPoint = sFieldName.indexOf((char) 0);
        if (nullPoint != -1) {
            sFieldName = sFieldName.substring(0, nullPoint);
        }

        return sFieldName;
    }

    /***
     * Write an 8-bit character to the file.
     * @param character Character to be written. 16-bit char will be converted to byte.
     * Note that data loss can occur if the character are bigger than 8 bits.
     * @throws IOException
     */
    public void write8bitChar(char character) throws IOException {
        writeByte((byte) character);
    }

    /***
     * Write an array of characters to the file. 16-bit characters will be converted to 8-bit characters.
     * @param chars An array of characters to be written to the file. Note that data loss can occur if characters are bigger than 8 bits.
     * @throws IOException
     */
    public void write8bitChars(char[] chars) throws IOException {
        for (int i = 0; i < chars.length; i++) {
            writeByte((byte) chars[i]);
        }
    }

}
