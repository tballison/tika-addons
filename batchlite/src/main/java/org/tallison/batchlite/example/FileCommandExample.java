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
package org.tallison.batchlite.example;

import org.apache.tika.utils.ProcessUtils;
import org.tallison.batchlite.AbstractDirectoryProcessor;
import org.tallison.batchlite.AbstractFileProcessor;
import org.tallison.batchlite.CommandlineFileProcessor;
import org.tallison.batchlite.MetadataWriter;
import org.tallison.batchlite.writer.MetadataWriterFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * This is an example of running the file command on a directory
 * of files
 */
public class FileCommandExample extends AbstractDirectoryProcessor {

    private final Path targRoot;
    private final MetadataWriter metadataWriter;
    private final int numThreads;
    public FileCommandExample(Path srcRoot, Path targRoot, MetadataWriter metadataWriter, int numThreads) {
        super(srcRoot);
        this.targRoot = targRoot;
        this.metadataWriter = metadataWriter;
        this.numThreads = numThreads;
    }

    @Override
    public List<AbstractFileProcessor> getProcessors(ArrayBlockingQueue<Path> queue) {
        List<AbstractFileProcessor> processors = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            processors.add(new FileProcessor(queue, getRootDir(), targRoot, metadataWriter));
        }
        return processors;
    }

    private class FileProcessor extends CommandlineFileProcessor {
        public FileProcessor(ArrayBlockingQueue<Path> queue, Path srcRoot,
                             Path targRoot, MetadataWriter metadataWriter) {
            super(queue, srcRoot, targRoot, metadataWriter);
        }

        @Override
        protected String[] getCommandLine(Path srcPath, Path targPath) {
            return new String[]{
                    "file",
                    "-b", "--mime-type",
                    ProcessUtils.escapeCommandLine(srcPath.toAbsolutePath().toString())
            };
        }
    }

    public static void main(String[] args) throws Exception {
        Path srcRoot = Paths.get(args[0]);
        Path targRoot = Paths.get(args[1]);
        String metadataWriterString = args[2];
        int numThreads = 10;
        if (args.length > 3) {
            numThreads = Integer.parseInt(args[3]);
        }
        long start = System.currentTimeMillis();
        MetadataWriter writer = MetadataWriterFactory.build(metadataWriterString);
        try {
            FileCommandExample runner = new FileCommandExample(srcRoot, targRoot, writer, numThreads);
            //runner.setMaxFiles(100);
            runner.execute();
        } finally {
            writer.close();
            long elapsed = System.currentTimeMillis()-start;
            System.out.println("Processed "+ writer.getRecordsWritten() + " files in "+
                    elapsed + " ms.");
        }
    }
}
