package org.yah.benchmark.nio.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static org.yah.benchmark.nio.reader.FloatChunkReaderSupport.ceilDiv;

/**
 * Map {@link MappedByteBuffer} over the requested slice at each access.
 */
public final class MappedFileFloatChunkReader extends BaseFloatChunkReader {

    private static final int CHUNK_FLOATS = Integer.MAX_VALUE / Float.BYTES;

    public MappedFileFloatChunkReader(Path filePath) throws IOException {
        super(filePath);
    }

    @Override
    public void read(float[] dst, long srcIndex, int dstIndex, int length) throws IOException {
        // from doc size – The size of the region to be mapped; must be non-negative and no greater than Integer.MAX_VALUE
        // needs chunks if > Integer.MAX_VALUE
        int chunks = ceilDiv(length, CHUNK_FLOATS);
        long srcOffset = srcIndex * (long) Float.BYTES;
        for (int i = 0; i < chunks; i++) {
            int chunkFloats = Math.min(length, CHUNK_FLOATS);
            ByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, srcOffset, chunkFloats * (long) Float.BYTES).order(ByteOrder.nativeOrder());
            buffer.asFloatBuffer().get(0, dst, dstIndex, chunkFloats);
            srcOffset += chunkFloats * (long) Float.BYTES;
            length -= chunkFloats;
            dstIndex += chunkFloats;
        }
    }

    @Override
    public void close() throws IOException {
        // clear any pending byte buffer
        System.gc();
        super.close();
    }

}
