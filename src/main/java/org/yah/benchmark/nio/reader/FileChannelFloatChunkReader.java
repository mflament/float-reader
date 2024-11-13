package org.yah.benchmark.nio.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 * Use a FileChannel (not locked, file can be concurrently updated) on the storage file and a staging {@link java.nio.ByteBuffer}
 * to read from the file channel.<br/>
 * The ByteBuffer size can be configured, and can be direct or on java heap.<br/>
 * <strong>Not thread safe</strong>
 */
public final class FileChannelFloatChunkReader extends BaseFloatChunkReader {

    private final FileChannel fileChannel;
    private final int stagingCapacity;
    private ByteBuffer stagingBuffer;

    /**
     * @param filePath            the storage file path
     * @param stagingCapacity     the capacity (in float count) of the staging buffer
     * @param directStagingBuffer true to use direct {@link ByteBuffer} as staging buffer
     */
    public FileChannelFloatChunkReader(Path filePath, int stagingCapacity, boolean directStagingBuffer) throws IOException {
        super(filePath);
        fileChannel = openFileChannel();
        this.stagingCapacity = stagingCapacity;
        stagingBuffer = directStagingBuffer
                ? ByteBuffer.allocateDirect(stagingCapacity)
                : ByteBuffer.allocate(stagingCapacity);
        stagingBuffer.order(ByteOrder.nativeOrder());
    }

    @Override
    public long length() throws IOException {
        stagingBuffer.position(0).limit(Long.BYTES);
        fileChannel.read(stagingBuffer, 0L);
        return stagingBuffer.getLong(0);
    }

    @Override
    public void read(float[] dst, long srcIndex, int dstIndex, int length) throws IOException {
        fileChannel.position(Long.BYTES + srcIndex * Float.BYTES);
        int remaining = Math.min(length, dst.length - dstIndex), index = dstIndex;
        stagingBuffer.clear();
        FloatBuffer floatBuffer = stagingBuffer.asFloatBuffer();
        while (remaining > 0) {
            int chunkSize = Math.min(stagingCapacity, remaining);
            stagingBuffer.position(0).limit(chunkSize * Float.BYTES);
            while (stagingBuffer.hasRemaining()) fileChannel.read(stagingBuffer);
            floatBuffer.get(0, dst, dstIndex, chunkSize);
            dstIndex += chunkSize;
            remaining -= chunkSize;
        }
    }

    @Override
    public void close() throws IOException {
        // release staging buffers
        stagingBuffer = null;
        fileChannel.close();
    }
}
