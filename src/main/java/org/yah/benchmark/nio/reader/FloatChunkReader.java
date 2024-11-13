package org.yah.benchmark.nio.reader;

import java.io.IOException;

/**
 * Reade a chunk of floats from a huge file (stored in little endian).
 */
public interface FloatChunkReader extends AutoCloseable {

    /**
     * @return the number of floats in the storage file
     */
    long length() throws IOException;

    /**
     * Read length floats from a file, starting at float index srcIndex from storage, and writing to dstIndex in dst.
     *
     * @param dst       the destination array
     * @param srcIndex  the index of the float to start in the source file to start from
     * @param dstIndex the index in the dst to start writing to
     * @param length    the number of floats to read from source file and write to dst
     */
    void read(float[] dst, long srcIndex, int dstIndex, int length) throws IOException;

    default void read(float[] dst, long srcOffset) throws IOException {
        read(dst, srcOffset, 0, dst.length);
    }

    @Override
    void close() throws IOException;
}
