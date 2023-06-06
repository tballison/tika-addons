package org.tallison.tika.extracts;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.filter.MetadataFilter;
import org.apache.tika.metadata.serialization.JsonMetadataList;
import org.apache.tika.pipes.FetchEmitTuple;
import org.apache.tika.pipes.emitter.EmitData;
import org.apache.tika.pipes.emitter.EmitKey;
import org.apache.tika.pipes.emitter.Emitter;
import org.apache.tika.pipes.emitter.EmitterManager;
import org.apache.tika.pipes.fetcher.Fetcher;
import org.apache.tika.pipes.fetcher.FetcherManager;
import org.apache.tika.pipes.pipesiterator.CallablePipesIterator;
import org.apache.tika.pipes.pipesiterator.PipesIterator;

/**
 * Simple class to take a directory/pipesiterator of extracts
 * and emit them to another data source.
 *
 * The initial use case is to take a batch of extracts and
 * emit portions of them to a db.  For example, I want
 * to put pdfa compliance and other pdf features into separate
 * columns in a db based on an s3 bucket of extracts.
 */
public class ExtractEmitterCLI {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExtractEmitterCLI.class);
    private static final int MAX_CACHE_SIZE = 100;

    public static void main(String[] args) throws Exception {
        Path tikaConfig = Paths.get(args[0]);
        int numThreads = 10;
        if (args.length > 1) {
            numThreads = Integer.parseInt(args[1]);
        }
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads + 1);
        ExecutorCompletionService<Long> executorCompletionService =
                new ExecutorCompletionService<>(executorService);
        ArrayBlockingQueue<FetchEmitTuple> queue = new ArrayBlockingQueue<>(10000);

        executorCompletionService.submit(new CallablePipesIterator(
                PipesIterator.build(tikaConfig), queue));

        for (int i = 0; i < numThreads; i++) {
            executorCompletionService.submit(new ExtractWorker(queue, tikaConfig));
        }
        int finished = 0;
        try {
            while (finished <= numThreads) {
                Future<Long> future = executorCompletionService.take();
                future.get();
                finished++;
            }
        } finally {
            executorService.shutdown();
            executorService.shutdownNow();
        }
    }

    private static class ExtractWorker implements Callable<Long> {
        private final static AtomicLong COUNTER = new AtomicLong(0);
        private final ArrayBlockingQueue<FetchEmitTuple> queue;
        private final MetadataFilter metadataFilter;
        private final Fetcher fetcher;
        private final Emitter emitter;
        public ExtractWorker(ArrayBlockingQueue<FetchEmitTuple> queue, Path tikaConfig) throws Exception {
            this.queue = queue;
            metadataFilter = new TikaConfig(tikaConfig).getMetadataFilter();
            fetcher = FetcherManager.load(tikaConfig).getFetcher();
            //this is intentionally per thread for current jdbc implementation
            emitter = EmitterManager.load(tikaConfig).getEmitter();
        }

        @Override
        public Long call() throws Exception {
            List<EmitData> emitData = new ArrayList<>();
            while (true) {
                //blocking
                long start = System.currentTimeMillis();
                FetchEmitTuple tuple = queue.take();
                long elapsed = System.currentTimeMillis() - start;
                LOGGER.debug("took {}ms to get a tuple from the queue", elapsed);

                if (tuple == PipesIterator.COMPLETED_SEMAPHORE) {
                    queue.put(PipesIterator.COMPLETED_SEMAPHORE);
                    emitter.emit(emitData);
                    emitData.clear();
                    return 1l;
                }
                try {
                    process(tuple, emitData);
                    long processed = COUNTER.incrementAndGet();
                    if (processed % 100 == 0) {
                        LOGGER.info("processed {} files", processed);
                    }
                    if (emitData.size() >= MAX_CACHE_SIZE) {
                        start = System.currentTimeMillis();
                        emitter.emit(emitData);
                        elapsed = System.currentTimeMillis() - start;
                        LOGGER.debug("took {}ms to emit {}", elapsed, emitData.size());
                        emitData.clear();
                    }
                } catch (TikaException | IOException e) {
                    LOGGER.warn("problem with " + tuple.getId(), e);
                }
            }
        }

        private void process(FetchEmitTuple tuple, List<EmitData> emitData) throws TikaException,
                IOException {
            long start = System.currentTimeMillis();
            LOGGER.debug("processing " + tuple.getFetchKey().getFetchKey());
            List<Metadata> metadataList;
            Metadata metadata = new Metadata();
            try(TikaInputStream tikaInputStream =
                    (TikaInputStream) fetcher.fetch(tuple.getFetchKey().getFetchKey(),
                    metadata)) {
                long elapsed = System.currentTimeMillis() - start;

                LOGGER.debug("took {}ms to fetch tikaInputStream", elapsed);

                try (Reader reader = Files.newBufferedReader(tikaInputStream.getPath(),
                        StandardCharsets.UTF_8)) {
                    elapsed = System.currentTimeMillis() - start;
                    LOGGER.debug("took {}ms to open reader", elapsed);

                    metadataList = JsonMetadataList.fromJson(reader);
                    elapsed = System.currentTimeMillis() - start;
                    LOGGER.debug("took {}ms to download and parse", elapsed);
                }
            }

            for (Metadata m : metadataList) {
                metadataFilter.filter(m);
            }
            long elapsed = System.currentTimeMillis() - start;
            LOGGER.debug("took {}ms to filter", elapsed);
            //this stinks -- need to find a better way
            //of figuring out if we need to strip the .json
            String emitKey = tuple.getEmitKey().getEmitKey();
            if (emitKey.endsWith(".json")) {
                int i = emitKey.lastIndexOf(".json");
                if (i > -1) {
                    emitKey = emitKey.substring(0, i);
                }
            }
            emitData.add(new EmitData(
                    new EmitKey(tuple.getEmitKey().getEmitterName(), emitKey),
                    metadataList));
        }
    }
}
