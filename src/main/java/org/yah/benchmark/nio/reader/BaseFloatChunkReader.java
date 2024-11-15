package org.yah.benchmark.nio.reader;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Support class for all implementations of {@link FloatChunkReader}
 */
public abstract class BaseFloatChunkReader implements FloatChunkReader {
    protected final Path filePath;

    protected final FileChannel fileChannel;

    protected BaseFloatChunkReader(Path filePath) throws IOException {
        this.filePath = filePath;
        fileChannel = FileChannel.open(filePath, StandardOpenOption.READ);
    }

    @Override
    public final long length() throws IOException {
        return fileChannel.size() / Float.BYTES;
    }

    @Override
    public void close() throws IOException {
        fileChannel.close();
    }
}
