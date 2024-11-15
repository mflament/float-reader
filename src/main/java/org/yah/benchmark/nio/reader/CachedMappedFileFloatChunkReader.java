package org.yah.benchmark.nio.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * Use multiple {@link MappedByteBuffer}, and create slice of {@link java.nio.FloatBuffer} from this mapped
 * buffer when reading a chunk.<br/>
 * MappedByteBuffer are cached when first read.
 */
public final class CachedMappedFileFloatChunkReader extends BaseFloatChunkReader {

    private static final int CHUNK_FLOATS = Integer.MAX_VALUE / Float.BYTES;
    private static final int CHUNK_SIZE = CHUNK_FLOATS * Float.BYTES; // max size of a mapped byte buffer (despite size param is a long ...)
    private static final int MAX_BUFFERS = 100; // "100 buffers ought to be enough for anybody"

    /**
     * MappedByteBuffers per slice of (Integer.MAX_VALUE / 4) floats (first also contains length, so 2 floats less)
     */
    private final ByteBuffer[] bufferChunks = new ByteBuffer[MAX_BUFFERS];

    public CachedMappedFileFloatChunkReader(Path filePath) throws IOException {
        super(filePath);
    }

    @Override
    public void read(float[] dst, long srcIndex, int dstIndex, int length) throws IOException {
        int chunkIndex = getBufferIndex(srcIndex);
        ByteBuffer buffer = getBufferChunk(chunkIndex);

        long bufferStartIndex = chunkIndex * (long) CHUNK_FLOATS;
        int indexInChunk = (int) (srcIndex - bufferStartIndex);
        if (indexInChunk < 0) throw new IllegalStateException("invalid index in chunk " + indexInChunk);

        FloatBuffer slice = buffer.asFloatBuffer();
        int lengthInChunk = Math.min(length, slice.capacity() - indexInChunk);
        buffer.asFloatBuffer().get(indexInChunk, dst, dstIndex, lengthInChunk);
        if (lengthInChunk < length) {
            buffer = getBufferChunk(chunkIndex + 1);
            int remaining = length - lengthInChunk;
            buffer.asFloatBuffer().get(0, dst, dstIndex + lengthInChunk, remaining);
        }
    }

    private ByteBuffer getBufferChunk(int chunkIndex) throws IOException {
        if (bufferChunks[chunkIndex] == null) {
            bufferChunks[chunkIndex] = mapBuffer(chunkIndex);
        }
        return bufferChunks[chunkIndex];
    }

    @Override
    public void close() throws IOException {
        Arrays.fill(bufferChunks, null);
        System.gc();
        super.close();
    }

    private ByteBuffer mapBuffer(int bufferIndex) throws IOException {
        long fileSize = fileChannel.size();
        long startOffset = bufferIndex * (long) CHUNK_SIZE;
        if (startOffset >= fileSize)
            throw new IllegalArgumentException(String.format("buffer %d startOffset %s overflow file size %d", bufferIndex, startOffset, fileSize));
        long size = Math.min(CHUNK_SIZE, fileSize - startOffset);
        return fileChannel.map(FileChannel.MapMode.READ_ONLY, startOffset, size).order(ByteOrder.nativeOrder());
    }

    /**
     * @param floatIndex index of float in file storage
     * @return index of mapped byte buffer containing this float
     */
    private static int getBufferIndex(long floatIndex) {
        long bufferIndex = floatIndex / CHUNK_FLOATS;
        if (bufferIndex > MAX_BUFFERS)
            throw new IllegalArgumentException(String.format("buffer index %d for float index %d overflow max buffers count %d", bufferIndex, floatIndex, MAX_BUFFERS));
        return (int) bufferIndex;
    }
}
