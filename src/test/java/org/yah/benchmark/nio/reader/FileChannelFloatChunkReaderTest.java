package org.yah.benchmark.nio.reader;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.yah.benchmark.nio.writer.FloatStorageGenerator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FileChannelFloatChunkReaderTest extends AbstractFloatChunkReaderTest {

    private static final int STAGING_CAPACITY = 1024 * 1024;

    @ParameterizedTest
    @ValueSource(longs = {1, 2000, Integer.MAX_VALUE + 5000L})
    void testHeapBuffer(long count) throws IOException {
        test(testFile -> createReader(testFile,false), count);
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 2000, Integer.MAX_VALUE + 5000L})
    void testDirectBuffer(long count) throws IOException {
        test(testFile -> createReader(testFile,true), count);
    }

    @Test
    public void testReadOverflow() throws IOException {
        testReadOverflow(FileChannelFloatChunkReader::new);
    }

    // test when staging buffer gets refilled
    @Test
    void testRefillStagingBuffer() throws IOException {
        long count = 2000;
        Path testFile = getTestFilePath(count);
        if (!Files.exists(testFile))
            FloatStorageGenerator.generate(testFile, count, createProducerFactory(count), false, 4);
        FloatStorageGenerator.FloatProducer producer = createProducer(count);
        try (FloatChunkReader reader = new FileChannelFloatChunkReader(testFile, 300, false)) {
            assertEquals(count, reader.length());
            float[] dst = new float[700];
            reader.read(dst, 0);
            checkFloats(producer, 0, dst, 0, 1);
        }
    }



    private FileChannelFloatChunkReader createReader(Path testFile, boolean direct) throws IOException {
        return new FileChannelFloatChunkReader(testFile, STAGING_CAPACITY, direct);
    }
}