package org.yah.benchmark.nio.reader;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.file.Path;

/**
 * Use a FileChannel (not locked, file can be concurrently updated) on the storage file and a staging {@link java.nio.ByteBuffer}
 * to read from the file channel.<br/>
 * The ByteBuffer size can be configured, and can be direct or on java heap.<br/>
 * <strong>Not thread safe</strong>
 */
public final class FileChannelFloatChunkReader extends BaseFloatChunkReader {

    private final int stagingCapacity;
    private ByteBuffer stagingBuffer;

    /**
     * @param filePath the storage file path
     */
    public FileChannelFloatChunkReader(Path filePath) throws IOException {
        this(filePath, 1024 * 1024, false);
    }

    /**
     * @param filePath            the storage file path
     * @param stagingCapacity     the capacity (in float count) of the staging buffer
     * @param directStagingBuffer true to use direct {@link ByteBuffer} as staging buffer
     */
    public FileChannelFloatChunkReader(Path filePath, int stagingCapacity, boolean directStagingBuffer) throws IOException {
        super(filePath);
        this.stagingCapacity = stagingCapacity;
        stagingBuffer = directStagingBuffer
                ? ByteBuffer.allocateDirect(stagingCapacity * Float.BYTES)
                : ByteBuffer.allocate(stagingCapacity * Float.BYTES);
        stagingBuffer.order(ByteOrder.nativeOrder());
    }

    @Override
    public void read(float[] dst, long srcIndex, int dstIndex, int length) throws IOException {
        fileChannel.position(srcIndex * Float.BYTES);
        // cap length: , and file capacity from src position
        int remaining = length;
        if (remaining > dst.length - dstIndex)
            remaining = dst.length - dstIndex; // no more than dst capacity from dstIndex
        long srcCapacity = (fileChannel.size() - fileChannel.position()) / Float.BYTES;
        if (remaining > srcCapacity)
            remaining = (int) srcCapacity; // no more than file length from srcIndex
        stagingBuffer.clear();
        FloatBuffer floatBuffer = stagingBuffer.asFloatBuffer();
        while (remaining > 0) {
            int chunkSize = Math.min(stagingCapacity, remaining);
            stagingBuffer.position(0).limit(chunkSize * Float.BYTES);
            while (stagingBuffer.hasRemaining())
                fileChannel.read(stagingBuffer);
            floatBuffer.get(0, dst, dstIndex, chunkSize);
            dstIndex += chunkSize;
            remaining -= chunkSize;
        }
    }

    @Override
    public void close() throws IOException {
        // release staging buffers
        stagingBuffer = null;
        super.close();
    }
}
