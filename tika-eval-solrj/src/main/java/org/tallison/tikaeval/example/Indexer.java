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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FilenameUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.serialization.JsonMetadataList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Indexer {
    private static final int DEFAULT_NUMBER_OF_THREADS = 5;
    private static final int SECOND_DELAY = 1;
    private static final int MAX_ITERATIONS = 120;//3 minutes

    private static final Path POISON = Paths.get("");

    private static final Logger LOG = LoggerFactory.getLogger(Indexer.class);


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
                        .build());
    }

    public static void main(String[] args) throws Exception {
        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(OPTIONS, args);
        TikaClient tikaClient = TikaClientFactory.getClient(commandLine);
        Path rootDir = TikaClientFactory.getRootDir(commandLine);
        Indexer indexer = new Indexer();
        int numThreads = (commandLine.hasOption("n")) ?
                Integer.parseInt(commandLine.getOptionValue("n")) :
                DEFAULT_NUMBER_OF_THREADS;
        indexer.execute(rootDir, tikaClient,
                commandLine.getOptionValue("s"), numThreads);
    }

    private void execute(Path extractsDir,
                         TikaClient tikaClient,
                         String solrUrl, int numThreads) throws IOException {
        try (SolrClient solrClient =
                     new ConcurrentUpdateSolrClient.Builder(solrUrl).build()) {
            try {
                solrClient.deleteByQuery("*:*");
                solrClient.commit();
            } catch (SolrServerException e) {
                LOG.warn("problem deleting", e);
            }
            //this is expensive. Only use one.
            DocMapper docMapper = new TikaEvalDocMapper();
            ExecutorService service = Executors.newFixedThreadPool(numThreads);
            ExecutorCompletionService<Integer> executorCompletionService = new ExecutorCompletionService<>(service);

            ArrayBlockingQueue<Path> extracts = new ArrayBlockingQueue<>(1000);
            executorCompletionService.submit(new PathCrawler(extractsDir, extracts, numThreads));
            LOG.info("numThreads: "+numThreads);
            long start = System.currentTimeMillis();
            for (int i = 0; i < numThreads; i++) {
                executorCompletionService.submit(new IndexerWorker(tikaClient, solrClient, extracts, docMapper));
            }

            int completed = 0;
            while (completed <= numThreads) {
                try {
                    Future<Integer> future = executorCompletionService.take();
                    Integer val = future.get();
                    completed++;
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
            long elapsed = System.currentTimeMillis()-start;
            LOG.info("finished indexing in "+elapsed+ " milliseconds");
            service.shutdown();
            service.shutdownNow();
            try {
                solrClient.commit();
                elapsed = System.currentTimeMillis()-start;
                LOG.info("finished the commit in "+elapsed+" milliseconds");
            } catch (SolrServerException e) {
                LOG.warn("problem committing ", e);
            }
        }
    }

    private class IndexerWorker implements Callable<Integer> {

        private final ArrayBlockingQueue<Path> extracts;
        private final TikaClient tikaClient;
        private final SolrClient solrClient;
        private final DocMapper docMapper;

        IndexerWorker(TikaClient tikaClient, SolrClient solrClient, ArrayBlockingQueue<Path> extracts,
                      DocMapper docMapper) {
            this.tikaClient = tikaClient;
            this.solrClient = solrClient;
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
            try (InputStream is = Files.newInputStream(path)) {
                metadataList = tikaClient.parse(is);
            } catch (IOException | TikaClientException e) {
                LOG.warn("serious problem with parse", e);
            }
            if (metadataList != null) {
                addDocs(path, metadataList);
            }
        }

        private void addDocs(Path path, List<Metadata> metadataList) {
            String parentId = UUID.randomUUID().toString();
            for (int i = 0; i < metadataList.size(); i++) {
                String docId = (i == 0) ? parentId : UUID.randomUUID().toString();
                addDoc(path, parentId, docId, metadataList.get(i));
            }
        }

        private void addDoc(Path path, String parentId, String docId, Metadata metadata) {

            SolrInputDocument doc = docMapper.map(metadata);
            doc.setField("id", docId);
            doc.setField("parent_id", parentId);
            doc.setField("path",
                    FilenameUtils.separatorsToUnix(
                            path.toAbsolutePath().toString()));
            try {
                solrClient.add(doc);
            } catch (SolrServerException | IOException e) {
                LOG.warn("problem adding doc", e);
            }
        }

    }

    private class PathCrawler implements Callable<Integer> {
        private final Path root;
        private final ArrayBlockingQueue<Path> extracts;
        private final int numThreads;

        PathCrawler(Path root, ArrayBlockingQueue<Path> extracts, int numThreads) {
            this.root = root;
            this.extracts = extracts;
            this.numThreads = numThreads;
        }

        @Override
        public Integer call() throws Exception {
            Files.walkFileTree(root, new PathAdder(extracts));
            for (int i = 0; i < numThreads; i++) {
                for (int j = 0; j < 60; j++) {
                    try {
                        boolean added = extracts.offer(POISON, SECOND_DELAY, TimeUnit.SECONDS);
                        if (added) {
                            break;
                        }
                    } catch (InterruptedException e) {
                        LOG.warn("interrupted");
                    }
                    LOG.warn("couldn't add poison");
                    throw new RuntimeException("Couldn't add poison after 60 seconds");
                }
            }
            return 1;
        }
    }

    private class PathAdder implements FileVisitor {

        private final ArrayBlockingQueue<Path> extracts;

        PathAdder(ArrayBlockingQueue<Path> extracts) {
            this.extracts = extracts;
        }

        @Override
        public FileVisitResult preVisitDirectory(Object dir, BasicFileAttributes attrs) throws IOException {
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Object file, BasicFileAttributes attrs) throws IOException {
            for (int i = 0; i < MAX_ITERATIONS; i++) {
                try {
                    boolean added = extracts.offer((Path) file, 1, TimeUnit.SECONDS);
                    if (added) {
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
