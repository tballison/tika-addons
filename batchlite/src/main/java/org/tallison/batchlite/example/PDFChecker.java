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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * This is an example of running the file command on a directory
 * of files
 */
public class PDFChecker extends AbstractDirectoryProcessor {

    private final Path targRoot;
    private final int numThreads;
    public PDFChecker(Path srcRoot, Path targRoot, int numThreads) {
        super(srcRoot);
        this.targRoot = targRoot;
        this.numThreads = numThreads;
    }

    @Override
    public List<AbstractFileProcessor> getProcessors(ArrayBlockingQueue<Path> queue) {
        List<AbstractFileProcessor> processors = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            processors.add(new FileProcessor(queue, getRootDir(), targRoot));
        }
        return processors;
    }

    private class FileProcessor extends CommandlineFileProcessor {
        public FileProcessor(ArrayBlockingQueue<Path> queue, Path srcRoot, Path targRoot) {
            super(queue, srcRoot, targRoot);
        }

        @Override
        protected String[] getCommandLine(Path srcPath, Path targPath) throws IOException {
            if (!Files.isDirectory(targPath.getParent())) {
                Files.createDirectories(targPath.getParent());
            }
            return new String[]{
                    "/home/tallison/tools/pdfchecker/PDF_Checker/pdfchecker",
                    "--profile",
                    "/home/tallison/tools/pdfchecker/PDF_Checker/CheckerProfiles/everything.json",
                    "--input",
                    ProcessUtils.escapeCommandLine(srcPath.toAbsolutePath().toString()),
                    "-s",
                    ProcessUtils.escapeCommandLine(targPath.toAbsolutePath().toString())
            };
        }

        @Override
        protected String getExtension() {
            return ".json";
        }
    }

    public static void main(String[] args) throws Exception {
        Path srcRoot = Paths.get(args[0]);
        Path targRoot = Paths.get(args[1]);
        int numThreads = 10;
        if (args.length > 2) {
            numThreads = Integer.parseInt(args[2]);
        }
        PDFChecker runner = new PDFChecker(srcRoot, targRoot, numThreads);
        //runner.setMaxFiles(100);
        runner.execute();
    }
}
