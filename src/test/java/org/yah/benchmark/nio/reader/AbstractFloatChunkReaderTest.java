package org.yah.benchmark.nio.reader;

import org.yah.benchmark.nio.BaseTest;
import org.yah.benchmark.nio.writer.FloatStorageGenerator;
import org.yah.benchmark.nio.writer.FloatStorageGenerator.FloatProducer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;

abstract class AbstractFloatChunkReaderTest extends BaseTest {

    // test when reading after end of file
    protected final void testReadOverflow(FloatChunkReaderFactory factory) throws IOException {
        long count = 50;
        Path testFile = getTestFilePath(count);
        if (!Files.exists(testFile))
            FloatStorageGenerator.generate(testFile, count, createProducerFactory(count), false, 4);
        FloatStorageGenerator.FloatProducer producer = createProducer(count);
        try (FloatChunkReader reader = factory.create(testFile)) {
            float[] dst = new float[100];
            reader.read(dst, 0, 0, 100);
            checkFloats(producer, 0, dst, 0, 50);
        }
    }

    private static final int MAX_DST_LENGTH = 5000;

    protected final void test(FloatChunkReaderFactory factory, long count) throws IOException {
        Path testFile = getTestFilePath(count);
        if (!Files.exists(testFile))
            FloatStorageGenerator.generate(testFile, count, createProducerFactory(count), false, 4);
        FloatProducer producer = createProducer(count);
        try (FloatChunkReader reader = factory.create(testFile)) {
            assertEquals(count, reader.length());
            float[] dst = new float[count < MAX_DST_LENGTH ? (int) count : MAX_DST_LENGTH];
            reader.read(dst, 0);
            checkFloats(producer, 0, dst, 0, 1);

            if (count > 600) {
                reader.read(dst, 500, 0, 100);
                checkFloats(producer, 500, dst, 0, 100);

                reader.read(dst, 550, 50, 50);
                checkFloats(producer, 550, dst, 50, 50);
            }
            if (count > Integer.MAX_VALUE + 1000L) {
                reader.read(dst, Integer.MAX_VALUE);
                checkFloats(producer, Integer.MAX_VALUE, dst, 0, 1000);
            }
        }
    }

    protected static Path getTestFilePath(long count) {
        return Paths.get(String.format("target/test_floats_%d.dat", count));
    }

    protected static void checkFloats(FloatProducer producer, long startIndex, float[] dst, int dstIndex, int length) {
        for (int i = 0; i < length; i++) {
            long idx = startIndex + i;
            float expected = producer.produce(idx);
            assertEquals(expected, dst[dstIndex + i], "at index " + idx);
        }
    }
}