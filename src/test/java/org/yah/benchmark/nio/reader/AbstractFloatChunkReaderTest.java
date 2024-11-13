package org.yah.benchmark.nio.reader;

import org.yah.benchmark.nio.BaseTest;
import org.yah.benchmark.nio.writer.FloatStorageGenerator;
import org.yah.benchmark.nio.writer.FloatStorageGenerator.FloatProducer;

import java.io.IOException;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertEquals;

abstract class AbstractFloatChunkReaderTest extends BaseTest {

    private static final int STAGING_CAPACITY = 1024 * 1024;

    @FunctionalInterface
    protected interface ReaderFactory {
        FloatChunkReader create() throws IOException;
    }

    protected final void test(ReaderFactory factory, long count) throws IOException {
        FloatStorageGenerator.generate(testFile, count, createProducerFactory(count), false, 4);
        FloatProducer producer = createProducer(count);
        try (FloatChunkReader reader = factory.create()) {
            assertEquals(count, reader.length());
            float[] dst = new float[count < 1000 ? (int) count : 1000];
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
        } finally {
            Files.deleteIfExists(testFile);
        }
    }

    private void checkFloats(FloatProducer producer, long startIndex, float[] dst, int dstIndex, int length) {
        for (int i = 0; i < length; i++) {
            long idx = startIndex + i;
            float expected = producer.produce(idx);
            assertEquals(expected, dst[dstIndex + i]);
        }
    }
}