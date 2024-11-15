package org.yah.benchmark.nio.reader;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;

class ConcurrentFloatChunkReaderTest extends AbstractFloatChunkReaderTest {

    @ParameterizedTest
    @ValueSource(longs = {1, 2000, Integer.MAX_VALUE + 5000L})
    void test(long count) throws IOException {
        test(testFile -> new ConcurrentFloatChunkReader(testFile, CachedMappedFileFloatChunkReader::new, 4, 1000), count);
    }
}