package org.yah.benchmark.nio.reader;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.yah.benchmark.nio.reader.FloatChunkReaderSupport.ceilDiv;

/**
 * A {@link java.nio.channels.FileChannel} that delegate read of floats chunks to a delegate using a given number of threads.<br/>
 * threadCount is only the maximum, each thread will at least read minChunkSize floats
 */
public class ConcurrentFloatChunkReader implements FloatChunkReader {

    private final Path storagePath;
    private final FloatChunkReaderFactory factory;
    private final int threadCount;
    private final int minChunkSize;
    private final ExecutorService executorService;
    private final FloatChunkReader[] readers;

    public ConcurrentFloatChunkReader(Path storagePath, FloatChunkReaderFactory factory,
                                      int threadCount, int minChunkSize) throws IOException {
        if (threadCount <= 1)
            throw new IllegalStateException("invalid thread count " + threadCount + " must be > 1");
        this.storagePath = Objects.requireNonNull(storagePath, "storagePath is null");
        this.factory = Objects.requireNonNull(factory, "factory is null");
        this.threadCount = threadCount;
        this.minChunkSize = minChunkSize;
        executorService = Executors.newFixedThreadPool(threadCount - 1);
        readers = new FloatChunkReader[threadCount];
        readers[0] = factory.create(storagePath);
    }

    @Override
    public long length() throws IOException {
        return getThreadReader(0).length();
    }

    @Override
    public void read(float[] dst, long srcIndex, int dstIndex, int length) throws IOException {
        int chunksCount = Math.min(threadCount, ceilDiv(length, minChunkSize));
        int chunkSize = ceilDiv(length, chunksCount);
        @SuppressWarnings("unchecked") Future<Void>[] futures = new Future[chunksCount - 1];
        for (int i = 0; i < chunksCount - 1; i++) {
            int threadId = i;
            futures[i] = executorService.submit(() -> readChunk(threadId, dst, srcIndex, dstIndex, length, chunkSize));
        }
        readChunk(chunksCount - 1, dst, srcIndex, dstIndex, length, chunkSize);
        int remaining = futures.length;
        while (remaining > 0) {
            for (int i = 0; i < futures.length; i++) {
                Future<Void> future = futures[i];
                if (future != null && future.isDone()) {
                    try {
                        future.get();
                    } catch (ExecutionException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    remaining--;
                    futures[i] = null;
                }
            }
        }
    }

    private Void readChunk(int threadId, float[] dst, long srcIndex, int dstIndex, int length, int chunkSize) throws IOException {
        FloatChunkReader reader = getThreadReader(threadId);
        @SuppressWarnings("IntegerMultiplicationImplicitCastToLong") long chunkSrcIndex = srcIndex + threadId * chunkSize; // can not overflow
        int chunkDstIndex = dstIndex + threadId * chunkSize;
        int currentChunkSize = Math.min(chunkSize, length - chunkDstIndex);
        reader.read(dst, chunkSrcIndex, chunkDstIndex, currentChunkSize);
        return null;
    }

    @Override
    public void close() throws IOException {
        executorService.shutdown();
        for (int i = 0; i < readers.length; i++) {
            if (readers[i] != null) {
                readers[i].close();
                readers[i] = null;
            }
        }
    }

    private FloatChunkReader getThreadReader(int threadId) throws IOException {
        if (readers[threadId] == null) {
            readers[threadId] = factory.create(storagePath);
        }
        return readers[threadId];
    }

}
