package org.yah.benchmark.nio.reader;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.yah.benchmark.nio.writer.FloatStorageGenerator;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

class CachedMappedFileFloatChunkReaderTest extends AbstractFloatChunkReaderTest {

    @ParameterizedTest
    @ValueSource(longs = {1, 2000, Integer.MAX_VALUE + 5000L})
    void test(long count) throws IOException {
        test(CachedMappedFileFloatChunkReader::new, count);
    }

    @Test
    void testMappedSizeOverflow() throws IOException {
        int count = Integer.MAX_VALUE / Float.BYTES + 100; // overflow Integer.MAX_VALUE bytes by 400 bytes
        Path testFile = getTestFilePath(count);
        if (!Files.exists(testFile))
            FloatStorageGenerator.generate(testFile, count, createProducerFactory(count), false, 4);
        FloatStorageGenerator.FloatProducer producer = createProducer(count);
        try (FloatChunkReader reader = new CachedMappedFileFloatChunkReader(testFile)) {
            float[] dst = new float[count];
            reader.read(dst, 0);
            checkFloats(producer, 0, dst, 0, count);
        }
    }

    @Test
    public void testReadOverflow() throws IOException {
        testReadOverflow(FileChannelFloatChunkReader::new);
    }

}