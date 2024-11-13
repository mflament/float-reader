package org.yah.benchmark.nio.reader;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.file.Path;

/**
 * Use a {@link MappedByteBuffer} over the whole file, and create slice of {@link java.nio.FloatBuffer} from this mapped
 * buffer when reading a chunk.
 */
public final class MappedFileFloatChunkReader extends BaseFloatChunkReader {

    public MappedFileFloatChunkReader(Path filePath) {
        super(filePath);
    }

    @Override
    public long length() throws IOException {
        return 0;
    }

    @Override
    public void read(float[] dst, long srcIndex, int dstIndex, int length) throws IOException {

    }

    @Override
    public void close() throws IOException {

    }
}
