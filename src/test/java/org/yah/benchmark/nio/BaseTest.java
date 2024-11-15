package org.yah.benchmark.nio;

import org.yah.benchmark.nio.writer.FloatStorageGenerator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.function.IntFunction;

public class BaseTest {

    protected static FloatStorageGenerator.FloatProducer createProducer(long count) {
        return index -> count == 1 ? 0 : index / (float) (count - 1);
    }

    protected static IntFunction<FloatStorageGenerator.FloatProducer> createProducerFactory(long count) {
        return unused -> createProducer(count);
    }

    protected static void read(ByteBuffer dst, FileChannel src) throws IOException {
        while (dst.hasRemaining()) src.read(dst);
    }
}
