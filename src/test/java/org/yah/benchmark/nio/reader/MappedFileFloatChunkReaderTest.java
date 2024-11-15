package org.yah.benchmark.nio.reader;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;

public class MappedFileFloatChunkReaderTest extends AbstractFloatChunkReaderTest {

    @ParameterizedTest
    @ValueSource(longs = {1, 2000, Integer.MAX_VALUE + 5000L})
    void test(long count) throws IOException {
        test(MappedFileFloatChunkReader::new, count);
    }

    @Test
    public void testReadOverflow() throws IOException {
        testReadOverflow(FileChannelFloatChunkReader::new);
    }

}