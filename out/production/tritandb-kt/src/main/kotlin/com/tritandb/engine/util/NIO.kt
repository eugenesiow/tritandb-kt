import java.nio.ByteBuffer
import kotlin.experimental.or

/**
 * Created by eugene on 10/05/2017.
 */

class ByteBufferBitOutput(initialSize:Int = ByteBufferBitOutput.DEFAULT_ALLOCATION) {
    /**
     * Returns the underlying DirectByteBuffer
     *
     * @return ByteBuffer of type DirectByteBuffer
     */
    var byteBuffer:ByteBuffer
    private var b:Byte = 0
    private var bitsLeft:Int = java.lang.Byte.SIZE
    var totalBits = 0
    init{
        byteBuffer = ByteBuffer.allocateDirect(initialSize)
        b = byteBuffer.get(byteBuffer.position())
    }
    private fun expandAllocation() {
        val largerBB = ByteBuffer.allocateDirect(byteBuffer.capacity() * 2)
        byteBuffer.flip()
        largerBB.put(byteBuffer)
        largerBB.position(byteBuffer.capacity())
        byteBuffer = largerBB
    }
    private fun flipByte() {
        if (bitsLeft == 0)
        {
            byteBuffer.put(b)
            if (!byteBuffer.hasRemaining())
            {
                expandAllocation()
            }
            b = byteBuffer.get(byteBuffer.position())
            bitsLeft = java.lang.Byte.SIZE
        }
    }
    /**
     * Sets the next bit (or not) and moves the bit pointer.
     *
     * @param bit true == 1 or false == 0
     */
    fun writeBit(bit:Boolean) {
        if (bit)
        {
            b = b or (1 shl (bitsLeft - 1)).toByte()
        }
        bitsLeft--
        totalBits++
        flipByte()
    }
    /**
     * Writes the given long to the stream using bits amount of meaningful bits.
     *
     * @param value Value to be written to the stream
     * @param bits How many bits are stored to the stream
     */
    fun writeBits(value:Long, bits:Int) {
        while (bits > 0)
        {
            val bitsToWrite = if ((bits > bitsLeft)) bitsLeft else bits
            if (bits > bitsLeft)
            {
                val shift = bits - bitsLeft
                b = b or ((value shr shift) and ((1L shl bitsLeft) - 1L)).toByte()
            }
            else
            {
                val shift = bitsLeft - bits
                b = b or (value shl shift).toByte()
            }
//            bits -= bitsToWrite
            bitsLeft -= bitsToWrite
            totalBits += bitsToWrite
            flipByte()
        }
    }
    /**
     * Causes the currently handled byte to be written to the stream
     */
    fun flush() {
        bitsLeft = 0
        flipByte() // Causes write to the ByteBuffer
    }
    companion object {
        val DEFAULT_ALLOCATION = 4096
    }
}/**
 * Creates a new ByteBufferBitOutput with a default allocated size of 4096 bytes.
 */