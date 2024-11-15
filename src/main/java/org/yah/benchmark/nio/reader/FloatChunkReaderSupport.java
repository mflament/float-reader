package org.yah.benchmark.nio.reader;

public final class FloatChunkReaderSupport {
    private FloatChunkReaderSupport() {
    }

    static int ceilDiv(int a, int b) {
        return (a + b - 1) / b;
    }

}
