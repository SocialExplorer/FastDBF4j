package com.socialexplorer.util;

import java.io.*;

public class ByteUtils {
    public static byte[] long2byte(long l) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(Long.SIZE/8);
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeLong(l);
        byte[] result = baos.toByteArray();
        dos.close();
        return result;
    }

    public static long byte2long(byte[] b) throws IOException {
        ByteArrayInputStream baos = new ByteArrayInputStream(b);
        DataInputStream dos = new DataInputStream(baos);
        long result = dos.readLong();
        dos.close();
        return result;
    }

    public static byte[] int2byte(int integ) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(Integer.SIZE/8);
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeInt(integ);
        byte[] result = baos.toByteArray();
        dos.close();
        return result;
    }

    public static int byte2int(byte[] b) throws IOException {
        ByteArrayInputStream baos = new ByteArrayInputStream(b);
        DataInputStream dos = new DataInputStream(baos);
        int result = dos.readInt();
        dos.close();
        return result;
    }

    public static byte[] short2byte(short s) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(Short.SIZE/8);
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeShort(s);
        byte[] result = baos.toByteArray();
        dos.close();
        return result;
    }

    public static int byte2short(byte[] b) throws IOException {
        ByteArrayInputStream baos = new ByteArrayInputStream(b);
        DataInputStream dos = new DataInputStream(baos);
        int result = dos.readUnsignedShort();
        dos.close();
        return result;
    }

    public static void reverseBytes(byte[] array) {
        if (array == null) {
            return;
        }
        int i = 0;
        int j = array.length - 1;
        byte tmp;
        while (j > i) {
            tmp = array[j];
            array[j] = array[i];
            array[i] = tmp;
            j--;
            i++;
        }
    }

    /***
     * Convert a signed integer to unsigned long. Use long to avoid integer overflow.
     * @param signedInt signed integer
     * @return unsigned value of the number
     */
    public static long getUnsigned(int signedInt) {
        return (signedInt & 0x00000000ffffffffL);
    }
}
