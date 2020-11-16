package org.tallison.batchlite;

import java.io.Closeable;
import java.io.IOException;

/**
 * Implementations must be thread safe!
 */
public interface MetadataWriter extends Closeable {

    void write(String relPath, FileProcessResult result) throws IOException;

    int getRecordsWritten();
}
