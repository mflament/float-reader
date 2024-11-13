package org.yah.benchmark.nio.reader;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;

public class FileChannelFloatChunkReaderTest extends AbstractFloatChunkReaderTest {

    private static final int STAGING_CAPACITY = 1024 * 1024;

    @ParameterizedTest
    @ValueSource(longs = {1, 2000, Integer.MAX_VALUE + 5000L})
    void testHeapBuffer(long count) throws IOException {
        test(() -> createReader(false), count);
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 2000, Integer.MAX_VALUE + 5000L})
    void testDirectBuffer(long count) throws IOException {
        test(() -> createReader(true), count);
    }

    private FileChannelFloatChunkReader createReader(boolean direct) throws IOException {
        return new FileChannelFloatChunkReader(testFile, STAGING_CAPACITY, direct);
    }
}