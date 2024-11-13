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

    protected BaseFloatChunkReader(Path filePath) {
        this.filePath = filePath;
    }

    protected final FileChannel openFileChannel() throws IOException {
        return FileChannel.open(filePath, StandardOpenOption.READ);
    }
}
