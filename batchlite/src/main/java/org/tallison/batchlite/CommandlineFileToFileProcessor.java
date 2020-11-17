package org.tallison.batchlite;

import com.google.gson.Gson;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;

public abstract class CommandlineFileToFileProcessor extends FileToFileProcessor {
    private static final Gson GSON = new Gson();
    private static final long DEFAULT_TIMEOUT_MILLIS = 120000;
    private static final int DEFAULT_MAX_BUFFER = 100000;
    private long timeoutMillis = DEFAULT_TIMEOUT_MILLIS;
    private int maxBuffer = DEFAULT_MAX_BUFFER;
    public CommandlineFileToFileProcessor(ArrayBlockingQueue<Path> queue,
                                          Path srcRoot, Path targRoot,
                                          MetadataWriter metadataWriter) {
        super(queue, srcRoot, targRoot, metadataWriter);
    }

    @Override
    protected void process(String relPath,
                                    Path srcPath, Path outputPath, MetadataWriter metadataWriter) throws IOException {
        String[] commandline = getCommandLine(srcPath, outputPath);
        FileProcessResult r = ProcessExecutor.execute(new ProcessBuilder(commandline),
                timeoutMillis, maxBuffer);
        metadataWriter.write(relPath, r);
     }

    protected abstract String[] getCommandLine(Path srcPath, Path targPath) throws IOException;

}
