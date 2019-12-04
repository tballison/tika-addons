/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.tallison.tikaeval.example;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FilenameUtils;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * As currently configured, this expects file path processing and a few other features.
 * java -cp "bin/*" org.apache.tika.server.TikaServerCli -spawnChild -s -c tika_pdfs.xml -enableUnsecureFeatures -enableFileUrl -d md5,sha256
 */
public class Indexer {
    private static final int WITHIN_MS = 10000;
    private static final int DEFAULT_NUMBER_OF_THREADS = 6;
    private static final int SECOND_DELAY = 1;
    private static final int MAX_ITERATIONS = 120;//3 minutes

    private static final Path POISON = Paths.get("");

    private static final Logger LOG = LoggerFactory.getLogger(Indexer.class);

    private static AtomicInteger DOCS_INDEXED = new AtomicInteger(0);

    static Options OPTIONS = new Options();

    static {

        OPTIONS.addOption(Option.builder("t")
                .required(false)
                .hasArg()
                .desc("tika server url")
                .build())
                .addOption(Option.builder("e")
                        .required(false)
                        .desc("extracts dir")
                        .hasArg()
                        .build())
                .addOption(Option.builder("i")
                        .required(false)
                        .desc("input directory of raw binary files")
                        .hasArg()
                        .build())
                .addOption(Option.builder("s")
                        .required()
                        .hasArg()
                        .desc("solr url")
                        .build())
                .addOption(Option.builder("n")
                        .longOpt("numThreads")
                        .hasArg()
                        .required(false)
                        .desc("number of threads")
                        .build())
                .addOption(Option.builder("m")
                                .longOpt("maxDocs")
                                .hasArg(true)
                                .required(false)
                                .desc("maximum number of documents to index").build());
    }

    private final Path rootDir;
    private int maxDocs = -1;

    public Indexer(Path rootDir) {
        this.rootDir = rootDir;
    }
    public static void main(String[] args) throws Exception {
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(OPTIONS, args);
        TikaClient tikaClient = TikaClientFactory.getClient(commandLine);
        Path rootDir = TikaClientFactory.getRootDir(commandLine);
        Indexer indexer = new Indexer(rootDir);

        int numThreads = (commandLine.hasOption("n")) ?
                Integer.parseInt(commandLine.getOptionValue("n")) :
                DEFAULT_NUMBER_OF_THREADS;
        int maxDocs = (commandLine.hasOption("m")) ?
                Integer.parseInt(commandLine.getOptionValue("m")) :
                -1;
        indexer.setMaxDocs(maxDocs);
        indexer.execute(TikaClientFactory.getClient(commandLine),
                commandLine.getOptionValue("s"), numThreads);
    }

    private void setMaxDocs(int maxDocs) {
        this.maxDocs = maxDocs;
    }

    private void execute(TikaClient tikaClient,
                         String searchUrl, int numThreads) throws Exception {
        try (SearchClient searchClient = SearchClientFactory.getClient(searchUrl)) {
            try {
                searchClient.deleteAll();
            } catch (IOException e) {
                LOG.warn("problem deleting", e);
            }
            //this is expensive. Only use one.
            DocMapper docMapper = new TikaEvalDocMapper();
            ExecutorService service = Executors.newFixedThreadPool(numThreads);
            ExecutorCompletionService<Integer> executorCompletionService = new ExecutorCompletionService<>(service);

            ArrayBlockingQueue<Path> extracts = new ArrayBlockingQueue<>(1000);
            executorCompletionService.submit(new PathCrawler(extracts, numThreads));
            LOG.info("numThreads: " + numThreads);
            long start = System.currentTimeMillis();
            for (int i = 0; i < numThreads; i++) {
                executorCompletionService.submit(new IndexerWorker(tikaClient, searchClient,
                        extracts, docMapper));
            }

            int completed = 0;
            while (completed <= numThreads) {
                try {
                    Future<Integer> future = executorCompletionService.take();
                    Integer val = future.get();
                    completed++;
                } catch (InterruptedException | ExecutionException e) {
                    shutdown(start, service);
                    throw new RuntimeException(e);
                }
            }
            shutdown(start, service);
        }
    }

    private void shutdown(long start, ExecutorService service) {
        long elapsed = System.currentTimeMillis() - start;
        LOG.info("finished indexing in " + elapsed + " milliseconds");
        service.shutdown();
        service.shutdownNow();
        elapsed = System.currentTimeMillis() - start;
        LOG.info("finished the commit in " + elapsed + " milliseconds");

    }


    private class IndexerWorker implements Callable<Integer> {

        private final ArrayBlockingQueue<Path> extracts;
        private final TikaClient tikaClient;
        private final SearchClient searchClient;
        private final DocMapper docMapper;

        IndexerWorker(TikaClient tikaClient, SearchClient searchClient, ArrayBlockingQueue<Path> extracts,
                      DocMapper docMapper) {
            this.tikaClient = tikaClient;
            this.searchClient = searchClient;
            this.extracts = extracts;
            this.docMapper = docMapper;
        }

        @Override
        public Integer call() throws Exception {
            while (true) {
                Path path = null;
                for (int i = 0; i < MAX_ITERATIONS; i++) {
                    path = extracts.poll(1, TimeUnit.SECONDS);
                    if (path != null) {
                        break;
                    }
                }
                if (path == null) {
                    LOG.warn("couldn't get a path in time");
                    throw new RuntimeException("couldn't get a path in time");
                }
                if (path.equals(POISON)) {
                    return 1;
                }
                processFile(path);
            }
        }

        private void processFile(Path path) {
            List<Metadata> metadataList = null;
            try (TikaInputStream tis = TikaInputStream.get(path)) {
                long length = Files.size(path);
                long start = System.currentTimeMillis();
                metadataList = tikaClient.parse(tis);
                if (metadataList.size() > 0) {
                    metadataList.get(0).set(Metadata.CONTENT_LENGTH, Long.toString(length));
                }
                int total = DOCS_INDEXED.incrementAndGet();
                LOG.info("completed " + path + " in " + (System.currentTimeMillis() - start)
                        + " millis (" + total + " docs total)");
            } catch (IOException | TikaClientException e) {
                LOG.warn("serious problem with parse", e);
            }
            if (metadataList != null) {
                addDocs(path, metadataList);
            }
        }

        private void addDocs(Path path, List<Metadata> metadataList) {
            if (metadataList == null || metadataList.size() == 0) {
                return;
            }
            List<Metadata> mapped = docMapper.map(metadataList);
            addIds(path, mapped);

            long start = System.currentTimeMillis();
            try {
                searchClient.addDocs(mapped);
            } catch (IOException e) {
                LOG.warn("problem adding doc", e);
            }
            long elapsed = System.currentTimeMillis() - start;
            LOG.info("finished sending to the index in " + elapsed + " millis");

        }

        private void addIds(Path path, List<Metadata> metadataList) {
            String containerId = UUID.randomUUID().toString();
            for (int i = 0; i < metadataList.size(); i++) {
                Metadata m = metadataList.get(i);
                String docId = (i == 0) ? containerId : UUID.randomUUID().toString();

                m.set(searchClient.getIdField(), docId);
                m.set("container_id", containerId);
                m.set("container_path",
                        FilenameUtils.separatorsToUnix(
                                rootDir.relativize(path).toString()));
                //assume that the first directory under the root directory
                //represents the name of the collection/sub-corpus
                String collection = rootDir.relativize(path).getName(0).toString();
                m.set("collection", collection);
            }
        }
    }
    private class PathCrawler implements Callable<Integer> {
        private final ArrayBlockingQueue<Path> extracts;
        private final int numThreads;

        PathCrawler(ArrayBlockingQueue<Path> extracts, int numThreads) {
            this.extracts = extracts;
            this.numThreads = numThreads;
        }

        @Override
        public Integer call() throws Exception {
            Files.walkFileTree(rootDir, new PathAdder(extracts));
            for (int i = 0; i < numThreads; i++) {
                for (int j = 0; j < 120; j++) {
                    try {
                        boolean added = extracts.offer(POISON, SECOND_DELAY, TimeUnit.SECONDS);
                        if (added) {
                            break;
                        }
                    } catch (InterruptedException e) {
                        LOG.warn("interrupted");
                    }
                    LOG.warn("couldn't add poison");
                    throw new RuntimeException("Couldn't add poison after 120 seconds");
                }
            }
            return 1;
        }
    }

    private class PathAdder implements FileVisitor {

        private final ArrayBlockingQueue<Path> extracts;
        private int numVisited = 0;

        PathAdder(ArrayBlockingQueue<Path> extracts) {
            this.extracts = extracts;
        }

        @Override
        public FileVisitResult preVisitDirectory(Object dir, BasicFileAttributes attrs) throws IOException {
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Object file, BasicFileAttributes attrs) throws IOException {
            if (maxDocs > 0 && numVisited >= maxDocs) {
                return FileVisitResult.TERMINATE;
            }
            Path path = (Path)file;
            //ignore hidden files
            if (path.getFileName().startsWith(".")) {
                return FileVisitResult.CONTINUE;
            }
            for (int i = 0; i < MAX_ITERATIONS; i++) {
                try {
                    boolean added = extracts.offer(path, 1, TimeUnit.SECONDS);
                    if (added) {
                        numVisited++;
                        return FileVisitResult.CONTINUE;
                    }
                } catch (InterruptedException e) {
                    LOG.warn("interrupted");
                }
            }
            LOG.warn("tried for " + MAX_ITERATIONS + " seconds and gave up.");
            return FileVisitResult.TERMINATE;
        }

        @Override
        public FileVisitResult visitFileFailed(Object file, IOException exc) throws IOException {
            return FileVisitResult.CONTINUE;

        }

        @Override
        public FileVisitResult postVisitDirectory(Object dir, IOException exc) throws IOException {
            return FileVisitResult.CONTINUE;
        }


    }

}
