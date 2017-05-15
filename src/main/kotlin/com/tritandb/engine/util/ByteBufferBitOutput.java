package com.tritandb.engine.util;

import java.io.IOException;
import java.io.OutputStream;

/**
 * An implementation of BitOutput interface that uses off-heap storage.
 *
 * @author Michael Burman
 */
public class ByteBufferBitOutput {

    private OutputStream out;
    private byte b = 0;
    private int bitsLeft = Byte.SIZE;


    public ByteBufferBitOutput(OutputStream out) {
        this.out = out;
    }

    private void flipByte() {
        if(bitsLeft == 0) {
            try {
                out.write(b);
                bitsLeft = Byte.SIZE;
                b = 0;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Sets the next bit (or not) and moves the bit pointer.
     *
     * @param bit true == 1 or false == 0
     */
    public void writeBit(boolean bit) {
        if(bit) {
            b |= (1 << (bitsLeft - 1));
        }
        bitsLeft--;
        flipByte();
    }

    /**
     * Writes the given long to the stream using bits amount of meaningful bits.
     *
     * @param value Value to be written to the stream
     * @param bits How many bits are stored to the stream
     */
    public void writeBits(long value, int bits) {
        while(bits > 0) {
            int bitsToWrite = (bits > bitsLeft) ? bitsLeft : bits;
            if(bits > bitsLeft) {
                int shift = bits - bitsLeft;
                b |= (byte) ((value >> shift) & ((1 << bitsLeft) - 1));
            } else {
                int shift = bitsLeft - bits;
                b |= (byte) (value << shift);
            }
            bits -= bitsToWrite;
            bitsLeft -= bitsToWrite;
            flipByte();
        }
    }

    /**
     * Causes the currently handled byte to be written to the stream
     */
    public void flush() {
        bitsLeft = 0;
        flipByte(); // Causes write to the ByteBuffer
    }

}
