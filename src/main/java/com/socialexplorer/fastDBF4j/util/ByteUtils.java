package com.socialexplorer.fastDBF4j.util;

import java.io.*;

public class ByteUtils {
    public static byte[] int2byte(int intValue) throws IOException {
        ByteArrayOutputStream outStream = new ByteArrayOutputStream(Integer.SIZE/8);
        DataOutputStream dos = new DataOutputStream(outStream);
        dos.writeInt(intValue);
        byte[] result = outStream.toByteArray();
        dos.close();
        return result;
    }

    public static int byte2int(byte[] b) throws IOException {
        ByteArrayInputStream outStream = new ByteArrayInputStream(b);
        DataInputStream dos = new DataInputStream(outStream);
        int result = dos.readInt();
        dos.close();
        return result;
    }

    /***
     * Convert a signed integer to unsigned long. Use long to avoid integer overflow.
     * @param signedInt signed integer
     * @return unsigned value of the number
     */
    public static long getUnsigned(int signedInt) {
        return (signedInt & 0x00000000ffffffffL);
    }


    public static short swap(short value) {
        int b1 = value & 0xff;
        int b2 = (value >> 8) & 0xff;

        return (short) (b1 << 8 | b2 << 0);
    }

    public static int swap(int value) {
        int b1 = (value >>  0) & 0xff;
        int b2 = (value >>  8) & 0xff;
        int b3 = (value >> 16) & 0xff;
        int b4 = (value >> 24) & 0xff;

        return b1 << 24 | b2 << 16 | b3 << 8 | b4 << 0;
    }
}
