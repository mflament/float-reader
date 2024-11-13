package org.yah.benchmark.nio.writer;

import org.yah.benchmark.nio.reader.FloatChunkReader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.IntFunction;

/**
 * Utility class to generate float for testing {@link FloatChunkReader}.
 */
public final class FloatStorageGenerator {

    @FunctionalInterface
    public interface FloatProducer {

        float produce(long index);

        /**
         * called after chunk generation, allowing to release/close stuff
         */
        default void complete() {
        }
    }

    private FloatStorageGenerator() {
    }

    /**
     * Single threaded version of {@link #generate(Path, long, IntFunction, boolean, int)}
     */
    public static void generate(Path file, long count, FloatProducer producer, boolean append) throws IOException {
        generate(file, count, unused -> producer, append, 1);
    }

    /**
     * Generate a file usable by {@link FloatChunkReader}
     *
     * @param file            the file to generate
     * @param count           the number of float to generate, if 0 (why ?), file will not be created or touched
     * @param producerFactory the factory of {@link FloatProducer} per chunk index to create floats value for each index in the chunk
     *                        index will start at last index if append is true,
     *                        must be thread safe is maxThreads > 1
     * @param append          if true, will append to any existing file, otherwise will truncate existing file or create a new file.
     * @param maxThreads      the number of thread to use (producer must be thread safe if > 1).
     */
    public static void generate(Path file, long count, IntFunction<FloatProducer> producerFactory, boolean append, int maxThreads) throws IOException {
        if (count == 0)
            return;
        if (maxThreads <= 0)
            throw new IllegalArgumentException("Invalid maxThreads count " + maxThreads + ", must be > 0");

        long startIndex = allocateFile(file, count, append);

        long threadChunkSize = Math.max(1000, ceil_div(count, maxThreads)); // at least 1000 floats per thread
        int threadsCount = ceil_div(count, threadChunkSize) < maxThreads ? (int) (count / threadChunkSize) : maxThreads;
        if (threadsCount > 1) {
            ExecutorService executorService = Executors.newFixedThreadPool(threadsCount);
            try {
                LinkedList<Future<?>> futures = new LinkedList<>();
                for (int chunkIndex = 0; chunkIndex < threadsCount; chunkIndex++) {
                    FloatProducer producer = producerFactory.apply(chunkIndex);
                    long currentChunkSize = Math.min(threadChunkSize, count - chunkIndex * threadChunkSize);
                    long chunkStartIndex = startIndex + chunkIndex * threadChunkSize;
                    Future<?> future = executorService.submit(() -> {
                        try {
                            generateChunk(file, producer, chunkStartIndex, currentChunkSize);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
                    futures.add(future);
                }
                waitAndCheck(futures);
            } finally {
                executorService.shutdownNow();
            }
        } else {
            FloatProducer producer = producerFactory.apply(0);
            generateChunk(file, producer, startIndex, count);
        }


        // write new count (now that all floats are generated)
        try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.WRITE)) { // file should exist and position must be at start
            ByteBuffer countBuffer = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.nativeOrder());
            countBuffer.putLong(0, startIndex + count);
            write(fileChannel, countBuffer);
        }
    }

    /**
     * reserve new space for count floats to existing file or create a new file
     *
     * @return the start index of file
     */
    private static long allocateFile(Path file, long count, boolean append) throws IOException {
        long startIndex;
        FileChannel fileChannel;
        if (append && Files.exists(file) && Files.size(file) > Long.BYTES) { // appending to existing file, with at least one float
            // needs read to get count, do not APPEND, we will use the count to get start position
            fileChannel = FileChannel.open(file, StandardOpenOption.WRITE, StandardOpenOption.READ);
            // read count to get start index
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.nativeOrder());
            read(buffer, fileChannel);
            startIndex = buffer.flip().getLong(0);
        } else { // empty or new file
            fileChannel = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            startIndex = 0;
        }

        long startPosition = computeStartPosition(startIndex);
        // reserve space by writing as single byte at new end position
        long endPosition = startPosition + count * Float.BYTES;
        fileChannel.position(endPosition - 1);
        write(fileChannel, ByteBuffer.allocate(1));
        return startIndex;
    }

    // file position to start writing, after count and all existing float
    private static long computeStartPosition(long startIndex) {
        return Long.BYTES + startIndex * Float.BYTES;
    }

    @SuppressWarnings("PointlessArithmeticExpression") // pointless but explicit :-)
    private static void generateChunk(Path file, FloatProducer producer, long startIndex, long count) throws IOException {
        // one file channel per thread to avoid concurrent update of position and to allow concurrent write
        // (is this a good idea to write concurrently on a disk ? not sure)
        long startPosition = computeStartPosition(startIndex);
        float[] chunkData = new float[1 * MB / Float.BYTES]; // stage 1MB of float on heap before writing
        ByteBuffer stagingBuffer = ByteBuffer.allocate(1 * MB).order(ByteOrder.nativeOrder());
        FloatBuffer floatBuffer = stagingBuffer.asFloatBuffer(); // for bulk write
        try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.WRITE)) {
            fileChannel.position(startPosition);
            long remaining = count, index = startIndex;
            while (remaining > 0) {
                int chunkSize = remaining < chunkData.length ? (int) remaining : chunkData.length;
                for (int i = 0; i < chunkSize; i++)
                    chunkData[i] = producer.produce(index + i);
                floatBuffer.put(0, chunkData);
                // keep staging in sync with new float buffer size
                stagingBuffer.limit(chunkSize * Float.BYTES);
                write(fileChannel, stagingBuffer);
                stagingBuffer.position(0); // restore position
                index += chunkSize;
                remaining -= chunkSize;
            }
        }
    }

    // perhaps JEP 462 will make it cleaner, simpler
    private static void waitAndCheck(LinkedList<Future<?>> futures) throws IOException {
        Future<?> future;
        while ((future = futures.poll()) != null) {
            if (!future.isDone()) futures.offer(future);
            else { // need to check for error, even if no result is expected
                try {
                    future.get();
                } catch (ExecutionException e) {
                    if (e.getCause() instanceof UncheckedIOException uncheckedIOException)
                        throw uncheckedIOException.getCause();
                    throw new RuntimeException(e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    // read fully
    private static void read(ByteBuffer dst, FileChannel src) throws IOException {
        while (dst.hasRemaining()) src.read(dst);
    }

    // write fully
    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static void write(FileChannel dst, ByteBuffer src) throws IOException {
        while (src.hasRemaining()) dst.write(src);
    }

    private static long ceil_div(long a, long b) {
        long res = a / b;
        if (a % b > 0) res += 1;
        return res;
    }

    private static final int MB = 1024 * 1024;

}
