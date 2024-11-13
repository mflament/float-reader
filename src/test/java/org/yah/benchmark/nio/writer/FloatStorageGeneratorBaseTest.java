package org.yah.benchmark.nio.writer;

import org.junit.jupiter.api.Test;
import org.yah.benchmark.nio.BaseTest;
import org.yah.benchmark.nio.writer.FloatStorageGenerator.FloatProducer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.function.IntFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FloatStorageGeneratorBaseTest extends BaseTest {

    @Test
    void singleThreadGenerate() throws IOException {
        long count = 1000;
        FloatProducer producer = createProducer(count);
        FloatStorageGenerator.generate(testFile, count, producer, false);
        checkFile(count, producer);
    }

    @Test
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
    void concurrentGenerate() throws IOException {
        long count = 8005;
        IntFunction<FloatProducer> producerFactory = createProducerFactory(count);
        FloatStorageGenerator.generate(testFile, count, producerFactory, false, 4);
        checkFile(count, createProducer(count));
    }

    @Test
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
        assertEquals(Long.BYTES + count * Float.BYTES, Files.size(testFile));
        final int chunkSize = 1024 * 1024;
        ByteBuffer stagingBuffer = ByteBuffer.allocate(chunkSize).order(ByteOrder.nativeOrder());
        try (FileChannel fileChannel = FileChannel.open(testFile, StandardOpenOption.READ)) {
            stagingBuffer.limit(Long.BYTES);
            read(stagingBuffer, fileChannel);
            assertEquals(count, stagingBuffer.getLong(0));

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