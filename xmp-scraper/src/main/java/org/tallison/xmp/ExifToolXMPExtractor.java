package org.tallison.xmp;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.tika.Tika;
import org.apache.tika.utils.ProcessUtils;

public class ExifToolXMPExtractor {

    private static final Path END_SEMAPHORE = Paths.get("");
    private static final Logger LOGGER = LogManager.getLogger(ExifToolXMPExtractor.class);
    private static final AtomicInteger COUNTER = new AtomicInteger(0);

    public static void main(String[] args) throws Exception {
        Path input = Paths.get(args[0]);
        Path output = Paths.get(args[1]);
        int numThreads = (args.length > 2) ? Integer.parseInt(args[2]) : 10;
        ExifToolXMPExtractor ex = new ExifToolXMPExtractor();
        ex.execute(input, output, numThreads);
    }

    private void execute(Path inputRoot, Path outputRoot, int numThreads) throws Exception {
        ExecutorService es = Executors.newFixedThreadPool(numThreads + 1);
        ExecutorCompletionService<Integer> completionService = new ExecutorCompletionService<>(es);
        ArrayBlockingQueue<Path> queue = new ArrayBlockingQueue<>(1000);
        completionService.submit(new PathWalker(inputRoot, queue, numThreads));
        for (int i = 0; i < numThreads; i++) {
            completionService
                    .submit(new ExifToolWorker(queue, inputRoot, outputRoot));
        }
        int finished = 0;
        try {
            while (finished < numThreads + 1) {
                Future<Integer> f = completionService.take();
                f.get();
                finished++;
            }
        } finally {
            es.shutdownNow();
        }
    }

    private static class ExifToolWorker implements Callable<Integer> {
        private final ArrayBlockingQueue<Path> queue;
        private final Path inputRoot;
        private final Path outputRoot;
        private final Tika tika = new Tika();

        public ExifToolWorker(ArrayBlockingQueue<Path> queue, Path inputRoot, Path outputRoot) {
            this.queue = queue;
            this.inputRoot = inputRoot;
            this.outputRoot = outputRoot;
        }

        @Override
        public Integer call() throws Exception {
            while (true) {
                Path p = queue.take();
                if (p == END_SEMAPHORE) {
                    return 1;
                }
                process(p);
            }
        }

        private void process(Path p) {
            long start = System.currentTimeMillis();
            String[] args = {"exiftool", "-xmp", "-b",
                    ProcessUtils.escapeCommandLine(p.toAbsolutePath().toString())};
            ProcessBuilder pb = new ProcessBuilder(args);
            Path errFile = null;
            Path outFile = null;
            try {
                errFile = Files.createTempFile("xmp-ex-", "");
                outFile = Files.createTempFile("xmp-ex-", "");
                pb.redirectError(errFile.toFile());
                pb.redirectOutput(outFile.toFile());
                Process process = pb.start();
                boolean finished = process.waitFor(60, TimeUnit.SECONDS);
                if (!finished) {
                    LOGGER.warn("timeout {}", p.toAbsolutePath());
                    process.destroyForcibly();
                    Thread.sleep(1000);
                }
                if (process.exitValue() != 0) {
                    LOGGER.warn("bad exit value {} in {}",
                            process.exitValue(), p.toAbsolutePath());
                }
                if (Files.size(outFile) > 0) {
                    String mimeDir = getMimeDir(p);
                    write(mimeDir, inputRoot.relativize(p), outFile);
                }
            } catch (Exception e) {
                LOGGER.warn("problem "+p.toAbsolutePath(), e);
            } finally {
                try {
                    if (errFile != null) {

                        Files.delete(errFile);
                    }
                } catch (IOException e) {
                    //log
                }
                try {
                    if (outFile != null) {
                        Files.delete(outFile);
                    }
                } catch (IOException e) {
                    //log
                }
            }
            long elapsed = System.currentTimeMillis() - start;
            LOGGER.debug("{} in {}ms", p.toAbsolutePath(), elapsed);
            int cnt = COUNTER.incrementAndGet();
            if (cnt % 1000 == 0) {
                LOGGER.info("processed {} files", cnt);
            }
        }

        private void write(String mimeDir, Path relPath, Path tmpXMP) throws IOException {
            Path targ = outputRoot.resolve(mimeDir).resolve(relPath);
            targ = targ.getParent().resolve(relPath.getFileName().toString()+ ".xmp");

            if (!Files.isDirectory(targ.getParent())) {
                Files.createDirectories(targ.getParent());
            }
            Files.copy(tmpXMP, targ, StandardCopyOption.REPLACE_EXISTING);
        }

        private String getMimeDir(Path p) {
            try {
                String mime = tika.detect(p);
                if (mime.equals("application/pdf")) {
                    return "pdf";
                } else if (mime.equals("image/jpeg")) {
                    return "jpeg";
                } else if (mime.equals("image/tiff")) {
                    return "tiff";
                } else if (mime.equals("image/png")) {
                    return "png";
                } else if (mime.equals("image/vnd.adobe.photoshop")) {
                    return "photoshop";
                } else if (mime.equals("application/postscript")) {
                    return "postscript";
                } else if (mime.equals("image/heic")) {
                    return "heic";
                } else if (mime.equals("image/webp")) {
                    return "webp";
                } else if (mime.startsWith("audio/")) {
                    return "audio";
                } else if (mime.startsWith("video")) {
                    return "video";
                } else if (mime.startsWith("image/")) {
                    return "image";
                } else {
                    LOGGER.warn("unhandled mime {}: {}", p, mime);
                    return "other";
                }
            } catch (Exception e) {
                LOGGER.warn("mime exception: " + p.toAbsolutePath().toString(), e);
                return "other";
            }
        }

    }


    private class PathWalker implements Callable<Integer> {
        private final Path inputRoot;
        private final ArrayBlockingQueue<Path> queue;
        private final int numThreads;

        public PathWalker(Path inputRoot, ArrayBlockingQueue<Path> queue, int numThreads) {
            this.inputRoot = inputRoot;
            this.queue = queue;
            this.numThreads = numThreads;
        }

        @Override
        public Integer call() throws Exception {
            Files.walkFileTree(inputRoot, new PathWalker.PathVisitor(queue));
            for (int i = 0; i < numThreads; i++) {
                queue.put(END_SEMAPHORE);
            }
            return 1;
        }

        private class PathVisitor implements FileVisitor<Path> {
            public PathVisitor(ArrayBlockingQueue<Path> queue) {
            }

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                    throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                    throws IOException {
                try {
                    queue.put(file);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc)
                    throws IOException {
                return FileVisitResult.CONTINUE;
            }
        }
    }
}
