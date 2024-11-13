package org.yah.benchmark.nio;

import org.junit.jupiter.api.BeforeEach;
import org.yah.benchmark.nio.writer.FloatStorageGenerator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.IntFunction;

public class BaseTest {

    protected final Path testFile = Paths.get("target/test_floats.dat");

    @BeforeEach
    public void setup() throws IOException {
        // cleanup in case of failure (could do it after each, but useful to get the generated file for debugging)
        Files.deleteIfExists(testFile);
    }

    protected static FloatStorageGenerator.FloatProducer createProducer(long count) {
        return index -> count == 1 ? 0 : index / (float) (count - 1);
    }

    protected IntFunction<FloatStorageGenerator.FloatProducer> createProducerFactory(long count) {
        return unused -> createProducer(count);
    }

    protected static void read(ByteBuffer dst, FileChannel src) throws IOException {
        while (dst.hasRemaining()) src.read(dst);
    }
}
