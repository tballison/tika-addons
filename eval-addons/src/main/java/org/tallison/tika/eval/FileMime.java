package org.tallison.tika.eval;

import org.apache.commons.io.IOUtils;
import org.apache.tika.io.IOExceptionWithCause;
import org.apache.tika.utils.ProcessUtils;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.nio.charset.StandardCharsets.UTF_8;

public class FileMime {
    private static final long TIMEOUT_MS = 6000;

    public static void main(String[] args) throws Exception {
        System.out.println(detect(Paths.get(args[0])));
    }

    public static String detect(Path path) throws IOException, InterruptedException {
        String[] args = new String[]{
                "file", "-b", "--mime-type", ProcessUtils.escapeCommandLine(path.toAbsolutePath().toString())
        };
        ProcessBuilder builder = new ProcessBuilder(args);
        Process process = builder.start();
        StringStreamGobbler errorGobbler = new StringStreamGobbler(process.getErrorStream());
        StringStreamGobbler outGobbler = new StringStreamGobbler(process.getInputStream());
        Thread errorThread = new Thread(errorGobbler);
        Thread outThread = new Thread(outGobbler);
        errorThread.start();
        outThread.start();

        process.getErrorStream();
        process.getInputStream();

        boolean finished = process.waitFor(TIMEOUT_MS, TimeUnit.MILLISECONDS);
        if (! finished) {
            process.destroyForcibly();
            throw new IOExceptionWithCause(new TimeoutException("timed out"));
        }
        int exitValue = process.exitValue();
        if (exitValue != 0) {
            throw new IOExceptionWithCause(new RuntimeException("bad exit value"));
        }
        errorThread.join();
        outThread.join();
        return outGobbler.toString().trim();
    }

    public static class StringStreamGobbler implements Runnable {

        //plagiarized from org.apache.oodt's StreamGobbler
        private final BufferedReader reader;
        private volatile boolean running = true;
        private final StringBuilder sb = new StringBuilder();

        public StringStreamGobbler(InputStream is) {
            this.reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(is), UTF_8));
        }

        @Override
        public void run() {
            String line = null;
            try {
                while ((line = reader.readLine()) != null && this.running) {
                    sb.append(line);
                    sb.append("\n");
                }
            } catch (IOException e) {
                //swallow ioe
            }
        }

        public void stopGobblingAndDie() {
            running = false;
            IOUtils.closeQuietly(reader);
        }

        @Override
        public String toString() {
            return sb.toString();
        }

    }
}
