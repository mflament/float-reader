package org.yah.benchmark.nio.bench;

import org.yah.benchmark.nio.reader.CachedMappedFileFloatChunkReader;
import org.yah.benchmark.nio.reader.ConcurrentFloatChunkReader;
import org.yah.benchmark.nio.reader.FileChannelFloatChunkReader;
import org.yah.benchmark.nio.reader.FloatChunkReader;
import org.yah.benchmark.nio.reader.FloatChunkReaderFactory;
import org.yah.benchmark.nio.reader.MappedFileFloatChunkReader;
import org.yah.benchmark.nio.writer.FloatStorageGenerator;
import org.yah.benchmark.nio.writer.FloatStorageGenerator.FloatProducer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@SuppressWarnings({"ClassEscapesDefinedScope", "UnusedReturnValue", "PointlessArithmeticExpression"})
public class FloatChunkReaderBench {

    public static void main(String[] args) throws IOException {
        FloatChunkReaderBench bench = new FloatChunkReaderBench(LogLevel.RESULT, 1, 3, 1, 12 * MB / Float.BYTES);
        bench.setup();
        bench.run();
    }

    private static final int KB = 1024;
    private static final int MB = 1024 * KB;
    private static final long GB = 1024 * MB;

    private static final long BENCH_FILE_SIZE = 4 * GB;
    private static final long BENCH_FILE_FLOATS = BENCH_FILE_SIZE / Float.BYTES;

    private static final Path BENCH_STORAGE_FILE = Paths.get("target/bench_storage.dat");

    private static final long SEED = 12345;

    private final LogLevel logLevel;

    private final int warmups;
    private final int runs;

    private final int minChunkLength;
    private final int maxChunkLength;

    private final float[] dst;


    public FloatChunkReaderBench(LogLevel logLevel, int warmups, int runs, int minChunkLength, int maxChunkLength) {
        this.logLevel = logLevel;
        this.warmups = warmups;
        this.runs = runs;
        this.minChunkLength = minChunkLength;
        this.maxChunkLength = maxChunkLength;
        dst = new float[maxChunkLength];
    }

    public void setup() throws IOException {
        if (!Files.exists(BENCH_STORAGE_FILE) || Files.size(BENCH_STORAGE_FILE) != BENCH_FILE_SIZE) {
            if (logLevel == LogLevel.ALL) System.out.println("Creating bench file " + BENCH_STORAGE_FILE);
            FloatStorageGenerator.generate(BENCH_STORAGE_FILE, BENCH_FILE_FLOATS, this::createRandomFloatProducer, false, 4);
        }
    }

    public List<BenchResult> run() throws IOException {
        List<BenchResult> results = new ArrayList<>();
        results.addAll(run(true));
        results.addAll(run(false));
        return results;
    }

    private List<BenchResult> run(boolean randomAccess) throws IOException {
        List<BenchResult> results = new ArrayList<>();
        final int[] stagingCapacities = {(int) (0.5 * MB), 1 * MB, 2 * MB, 4 * MB};
        for (int stagingCapacity : stagingCapacities) {
            results.add(run(String.format("file channel direct staging %.1fMB", stagingCapacity / (double) MB), file -> new FileChannelFloatChunkReader(file, stagingCapacity, true), randomAccess));
            results.add(run(String.format("file channel heap staging %.1fMB", stagingCapacity / (double) MB), file -> new FileChannelFloatChunkReader(file, stagingCapacity, false), randomAccess));
        }
        results.add(run("mapped buffer", MappedFileFloatChunkReader::new, randomAccess));
        results.add(run("cached mapped buffer", CachedMappedFileFloatChunkReader::new, randomAccess));

        int maxThreads = Runtime.getRuntime().availableProcessors();
        final int[] minChunkSizes = {500, 1000, 2000, 4000, 8000, 16000};
        int i = 2;
        while (true) {
            int threadCount = i;
            for (int minChunkSize : minChunkSizes) {
                results.add(run(String.format("concurrent reader (%dT) (minChunkSize=%d)", i, minChunkSize), file -> new ConcurrentFloatChunkReader(file, CachedMappedFileFloatChunkReader::new, threadCount, minChunkSize), randomAccess));
            }
            if (i == maxThreads)
                break;
            i *= 2;
            if (i > maxThreads)
                i = maxThreads;
        }
        return results;
    }

    private BenchResult run(String readerName, FloatChunkReaderFactory readerFactory, boolean randomAccess) throws IOException {
        try (FloatChunkReader reader = readerFactory.create(BENCH_STORAGE_FILE)) {
            return run(readerName, reader, randomAccess);
        }
    }

    private BenchResult run(String readerName, FloatChunkReader reader, boolean randomAccess) throws IOException {
        if (logLevel == LogLevel.ALL)
            System.out.printf("%d warmup %s on %s ", warmups, randomAccess ? "random access" : "linear access", readerName);
        for (int i = 0; i < warmups; i++) {
            for (int srcIndex = 0; srcIndex < BENCH_FILE_FLOATS; srcIndex += maxChunkLength) {
                int length = Math.min(maxChunkLength, (int) (BENCH_FILE_FLOATS - srcIndex));
                reader.read(dst, srcIndex, 0, length);
            }
            if (logLevel == LogLevel.ALL) System.out.print(".");
        }
        if (logLevel == LogLevel.ALL) System.out.println();

        if (logLevel == LogLevel.ALL)
            System.out.printf("%d %s run of %s ", runs, randomAccess ? "random access" : "linear access", readerName);

        double totalTime = 0; // ms
        double totalReadMB = 0;
        for (int i = 0; i < runs; i++) {
            Random random = new Random(SEED); // runs must be all equals to get a meaningful average
            if (randomAccess) {
                int length = random.nextInt(minChunkLength, maxChunkLength + 1);
                long srcIndex = random.nextLong(BENCH_FILE_FLOATS - length);
                int dstIndex = length == maxChunkLength ? 0 : random.nextInt(maxChunkLength - length); // bound must be positive
                long start = System.nanoTime();
                reader.read(dst, srcIndex, dstIndex, length);
                totalTime += (System.nanoTime() - start) * 1E-6;
                totalReadMB += length * Float.BYTES / (double) MB;
            } else {
                for (int srcIndex = 0; srcIndex < BENCH_FILE_FLOATS; srcIndex += maxChunkLength) {
                    int length = Math.min(maxChunkLength, (int) (BENCH_FILE_FLOATS - srcIndex));
                    long start = System.nanoTime();
                    reader.read(dst, srcIndex, 0, length);
                    totalTime += (System.nanoTime() - start) * 1E-6;
                }
                totalReadMB += BENCH_FILE_SIZE / (double) MB;
            }
            if (logLevel == LogLevel.ALL) System.out.print(".");
        }
        if (logLevel == LogLevel.ALL) System.out.println();

        BenchResult result = new BenchResult(randomAccess, readerName, runs, totalTime, totalReadMB);
        if (logLevel.atLeast(LogLevel.RESULT)) System.out.println(result);
        return result;
    }

    private FloatProducer createRandomFloatProducer(int chunkIndex) {
        Random random = new Random(SEED + chunkIndex); // random is not performant in multi thread
        return unused -> random.nextFloat();
    }

    @SuppressWarnings("unused")
    private enum LogLevel {
        NONE,
        RESULT,
        ALL;

        public boolean atLeast(LogLevel logLevel) {
            return ordinal() >= logLevel.ordinal();
        }
    }

    public record BenchResult(boolean randomAccess, String readerName, int runs, double totalTime, double totalRead) {
        @Override
        public String toString() {
            return String.format("(%s) %-50s : rate=%.2fMB/s", randomAccess ? "random access" : "linear access", readerName, rate());
        }

        public double rate() {
            return totalRead / (totalTime / 1000.0);
        }
    }

}
