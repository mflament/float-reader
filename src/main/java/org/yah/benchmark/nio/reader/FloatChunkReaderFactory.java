package org.yah.benchmark.nio.reader;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Factory of {@link FloatChunkReader} from a file
 */
@FunctionalInterface
public interface FloatChunkReaderFactory {
    FloatChunkReader create(Path storageFile) throws IOException;
}
