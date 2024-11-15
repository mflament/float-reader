package org.yah.benchmark.nio.writer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.yah.benchmark.nio.BaseTest;
import org.yah.benchmark.nio.writer.FloatStorageGenerator.FloatProducer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.function.IntFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class FloatStorageGeneratorTest extends BaseTest {

    private static final Path testFile = Paths.get("target/test_floats.dat");

    @BeforeEach
    public void setup() throws IOException {
        // cleanup in case of failure (could do it after each, but useful to get the generated file for debugging)
        Files.deleteIfExists(testFile);
    }

    @Test
    @Order(0)
    void singleThreadGenerate() throws IOException {
        long count = 1000;
        FloatProducer producer = createProducer(count);
        FloatStorageGenerator.generate(testFile, count, producer, false);
        checkFile(count, producer);
    }

    @Test
    @Order(1)
    void singleThreadAppend() throws IOException {
        long count = 1000;
        FloatProducer producer = createProducer(count * 2);
        // appending to no file should generate a new file
        FloatStorageGenerator.generate(testFile, count, producer, true);
        checkFile(count, producer);

        FloatStorageGenerator.generate(testFile, count, producer, true);
        checkFile(2 * count, producer);
    }

    @Test
    @Order(2)
    void concurrentGenerate() throws IOException {
        long count = 8005;
        IntFunction<FloatProducer> producerFactory = createProducerFactory(count);
        FloatStorageGenerator.generate(testFile, count, producerFactory, false, 4);
        checkFile(count, createProducer(count));
    }

    @Test
    @Order(3)
    void concurrentAppend() throws IOException {
        long count = 8005;
        IntFunction<FloatProducer> producerFactory = createProducerFactory(count * 2);
        FloatStorageGenerator.generate(testFile, count, producerFactory, false, 4);
        checkFile(count, createProducer(count  * 2));

        FloatStorageGenerator.generate(testFile, count, producerFactory, false, 4);
        checkFile(count, createProducer(count * 2));
    }

    private void checkFile(long count, FloatProducer producer) throws IOException {
        assertTrue(Files.exists(testFile), "file not generated");
        assertEquals(count * Float.BYTES, Files.size(testFile));
        final int chunkSize = 1024 * 1024;
        ByteBuffer stagingBuffer = ByteBuffer.allocate(chunkSize).order(ByteOrder.nativeOrder());
        try (FileChannel fileChannel = FileChannel.open(testFile, StandardOpenOption.READ)) {
            long floatCount = fileChannel.size() / Float.BYTES;
            assertEquals(count, floatCount);
            stagingBuffer.position(stagingBuffer.limit());
            for (long i = 0; i < count; i++) {
                if (!stagingBuffer.hasRemaining()) {
                    // refuel
                    stagingBuffer.clear();
                    long remaining = count - i;
                    if (remaining < chunkSize)
                        stagingBuffer.limit((int) remaining * Float.BYTES);
                    read(stagingBuffer, fileChannel);
                    stagingBuffer.flip();
                }
                float actual = stagingBuffer.getFloat();
                float expected = producer.produce(i);
                assertEquals(expected, actual, "Invalid value at index " + i);
            }
        }
    }

}