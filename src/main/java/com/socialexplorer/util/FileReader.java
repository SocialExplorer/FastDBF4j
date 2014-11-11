package com.socialexplorer.util;

import java.io.*;

public class FileReader {
    /**
     * Reader/writer that is actually used for reading and writing to the file.
     */
    private final RandomAccessFile reader;

    public FileReader(RandomAccessFile reader) {
        this.reader = reader;
    }


    public int readLittleEndianInt() throws IOException {
        return ByteUtils.swap(reader.readInt());
    }

    public int readLittleEndianShort() throws IOException {
        return ByteUtils.swap(reader.readShort());
    }


    public void writeLittleEndianInt(int bigEndianInt) throws IOException {
        reader.writeInt(ByteUtils.swap(bigEndianInt));
    }


    public void writeLittleEndianShort(short bigEndianShort) throws IOException {
        reader.writeShort(ByteUtils.swap(bigEndianShort));
    }


    /***
     * Read numberOfChars 8-bit characters and create a string out of them. The string is terminated
     * with the NULL character.
     * @param numberOfChars
     * @return
     * @throws IOException
     */
    public String readChars(int numberOfChars, String charsetName) throws IOException {
        byte[] charBuffer = new byte[11];
        for (int j = 0; j <= 10; j++) {
            charBuffer[j] = reader.readByte();
        }
        String word = new String(charBuffer, charsetName);
        // remove the terminating NULL character (00h, or 0 decimal)
        int nullPoint = word.indexOf((char) 0);
        if (nullPoint != -1) {
            word = word.substring(0, nullPoint);
        }

        return word;
    }

    /***
     * Write an 8-bit character to the file.
     * @param character Character to be written. 16-bit char will be converted to byte.
     * Note that data loss can occur if the character are bigger than 8 bits.
     */
    public void write8bitChar(char character) throws IOException {
        reader.writeByte((byte) character);
    }

    /***
     * Write an array of characters to the file. 16-bit characters will be converted to 8-bit characters.
     * @param chars An array of characters to be written to the file. Note that data loss can occur if characters are bigger than 8 bits.
     */
    public void write8bitChars(char[] chars) throws IOException {
        for (int i = 0; i < chars.length; i++) {
            reader.writeByte((byte) chars[i]);
        }
    }

    public void close() throws IOException { reader.close(); }
    public long length() throws IOException { return reader.length(); }
    public void seek(long pos) throws IOException { reader.seek(pos); }
    public long getFilePointer() throws IOException { return reader.getFilePointer(); }
    public void write(byte[] b) throws IOException { reader.write(b); }
    public void write(int b) throws IOException { reader.write(b); }
    public void writeByte(int v) throws IOException { reader.writeByte(v); }
    public void write(byte[] b, int off, int len) throws IOException { reader.write(b, off, len); }
    public byte readByte() throws IOException { return reader.readByte(); }
    public int read(byte[] b, int off, int len) throws IOException { return reader.read(b, off, len); }
    public int skipBytes(int n) throws IOException { return reader.skipBytes(n); }
}
