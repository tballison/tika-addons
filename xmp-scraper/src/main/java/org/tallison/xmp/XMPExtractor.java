package org.tallison.xmp;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.tika.Tika;

public class XMPExtractor {

    private static final Path END_SEMAPHORE = Paths.get("");
    private static final Logger LOGGER = LogManager.getLogger(XMPExtractor.class);
    private static final AtomicInteger COUNTER = new AtomicInteger(0);
    public static void main(String[] args) throws Exception {
        Path input = Paths.get(args[0]);
        Path output = Paths.get(args[1]);
        int numThreads = (args.length > 2) ?
                Integer.parseInt(args[2]) : 10;
        XMPExtractor ex = new XMPExtractor();
        ex.execute(input, output, numThreads);
    }

    private void execute(Path inputRoot, Path outputRoot, int numThreads) throws Exception {
        ExecutorService es = Executors.newFixedThreadPool(numThreads+1);
        ExecutorCompletionService<Integer> completionService = new ExecutorCompletionService<>(es);
        ArrayBlockingQueue<Path> queue = new ArrayBlockingQueue<>(1000);
        completionService.submit(new PathWalker(inputRoot, queue, numThreads));
        for (int i = 0; i < numThreads; i++) {
            completionService.submit(new XMPScraperWorker(queue, inputRoot, outputRoot));
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

    private static class XMPScraperWorker implements Callable<Integer> {
        private final ArrayBlockingQueue<Path> queue;
        private final Path inputRoot;
        private final Path outputRoot;
        private final Tika tika = new Tika();

        public XMPScraperWorker(ArrayBlockingQueue<Path> queue,
                                Path inputRoot, Path outputRoot) {
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
            try (InputStream is = new BufferedInputStream(
                    Files.newInputStream(p))) {
                XMPScraper scraper = new XMPScraper(is);
                int i = 0;
                Path relPath = inputRoot.relativize(p);
                String mimeDir = null;
                for (XMPResult r : scraper) {
                    if (mimeDir == null) {
                        mimeDir = getMimeDir(p);
                    }
                    try {
                        write(mimeDir, relPath, r, i++);
                    } catch (IOException e) {
                        LOGGER.warn("write exception: "+p.toAbsolutePath(), e);
                    }
                }
            } catch (Exception e) {
                LOGGER.warn("scanner exception: "+p.toAbsolutePath(), e);
            }
            long elapsed = System.currentTimeMillis()-start;
            LOGGER.debug("{} in {}ms",
                    p.toAbsolutePath(), elapsed);
            int cnt = COUNTER.incrementAndGet();
            if (cnt % 1000 == 0) {
                LOGGER.info("processed {} files", cnt);
            }
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
                LOGGER.warn(
                        "mime exception: " + p.toAbsolutePath().toString(), e);
                return "other";
            }
        }

        private void write(String mimeDir, Path relPath, XMPResult r, int i) throws IOException {
            Path targ = outputRoot.resolve(mimeDir).resolve(relPath);
            targ = targ.getParent().resolve(relPath.getFileName().toString()
                    +"-"+i+".xmp");

            if (! Files.isDirectory(targ.getParent())) {
                Files.createDirectories(targ.getParent());
            }
            try (OutputStream os = new BufferedOutputStream(
                    Files.newOutputStream(targ))) {
                os.write(r.getHeader());
                os.write(r.getPayload());
                os.write(r.getTrailer());
            }
        }

    }

    private class PathWalker implements Callable<Integer> {
        private final Path inputRoot;
        private final ArrayBlockingQueue<Path> queue;
        private final int numThreads;
        public PathWalker(Path inputRoot,
                          ArrayBlockingQueue<Path> queue, int numThreads) {
            this.inputRoot = inputRoot;
            this.queue = queue;
            this.numThreads = numThreads;
        }

        @Override
        public Integer call() throws Exception {
            Files.walkFileTree(inputRoot, new PathVisitor(queue));
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
            public FileVisitResult visitFileFailed(Path file,
                                                   IOException exc)
                    throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir,
                                                      IOException exc)
                    throws IOException {
                return FileVisitResult.CONTINUE;
            }
        }
    }
}
