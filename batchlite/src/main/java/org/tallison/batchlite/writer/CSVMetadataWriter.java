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
package org.tallison.batchlite.writer;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.tika.io.IOExceptionWithCause;
import org.tallison.batchlite.FileProcessResult;
import org.tallison.batchlite.MetadataWriter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class CSVMetadataWriter implements MetadataWriter {


    private static final long MAX_POLL_SECONDS = 600;

    private static String[] HEADER = new String[]{
            "path", "exitValue", "isTimeout", "processTimeMillis",
            "stderr", "stderrLength", "stderrTruncated",
            "stdout", "stdoutLength", "stdoutTruncated"
    };

    private final CSVPrinter printer;
    private final ArrayBlockingQueue<PathResultPair> rows = new ArrayBlockingQueue<>(1000);
    private final ExecutorService executorService;
    private final ExecutorCompletionService<Integer> completionService;
    private final WriterThread writerThread;

    CSVMetadataWriter(Path csvFile) throws IOException {
        printer = new CSVPrinter(
                Files.newBufferedWriter(csvFile, StandardCharsets.UTF_8),
                CSVFormat.EXCEL);
        printer.printRecord(HEADER);
        executorService = Executors.newFixedThreadPool(1);
        writerThread = new WriterThread(rows, printer);
        completionService = new ExecutorCompletionService<Integer>(executorService);
        completionService.submit(writerThread);
    }

    @Override
    public void write(String relPath, FileProcessResult result) throws IOException {
        try {
            rows.offer(new PathResultPair(relPath, result), MAX_POLL_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new IOExceptionWithCause(e);
        }
    }

    @Override
    public int getRecordsWritten() {
        return writerThread.getRecordsProcessed();
    }

    @Override
    public void close() throws IOException {
        rows.add(PathResultPair.POISON);
        try {

            Future<Integer> future = completionService.poll(MAX_POLL_SECONDS, TimeUnit.SECONDS);
            if (future == null) {
                throw new IOExceptionWithCause(
                        new TimeoutException("exceeded " + MAX_POLL_SECONDS
                                + " seconds"));
            }
            //trigger execution exception reporting
            future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IOExceptionWithCause(e);
        } finally {
            executorService.shutdownNow();
        }
        printer.flush();
        printer.close();
    }


    private static class WriterThread implements Callable<Integer> {
        int recordsProcessed = 0;
        private ArrayBlockingQueue<PathResultPair> queue;
        private final CSVPrinter printer;

        WriterThread(ArrayBlockingQueue<PathResultPair> queue, CSVPrinter printer) {
            this.queue = queue;
            this.printer = printer;
        }

        @Override
        public Integer call() throws Exception {
            while (true) {
                PathResultPair pair = queue.poll(MAX_POLL_SECONDS, TimeUnit.SECONDS);
                if (pair == null) {
                    throw new TimeoutException("waited longer than " + MAX_POLL_SECONDS
                            + " seconds");
                }
                if (pair == PathResultPair.POISON) {
                    return 1;
                }
                List<String> cols = new ArrayList<>();
                FileProcessResult result = pair.getResult();
                cols.add(pair.getRelPath());
                cols.add(Integer.toString(result.getExitValue()));
                cols.add(Boolean.toString(result.isTimeout()));
                cols.add(Long.toString(result.getProcessTimeMillis()));
                cols.add(result.getStderr());
                cols.add(Long.toString(result.getStderrLength()));
                cols.add(Boolean.toString(result.isStderrTruncated()));
                cols.add(result.getStdout());
                cols.add(Long.toString(result.getStdoutLength()));
                cols.add(Boolean.toString(result.isStdoutTruncated()));
                printer.printRecord(cols);
                if (++recordsProcessed % 1000 == 0) {
                    System.out.println("processed "+recordsProcessed + " records");
                }
            }
        }

        int getRecordsProcessed() {
            return recordsProcessed;
        }
    }
}
