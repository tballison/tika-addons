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
package org.tallison.batchlite;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * This takes an input file and runs {@link #process(String, Path, Path, Path)}
 * on the input file, stores the metadata in targRoot/metadata
 * and the output (if there is any) in targRoot/output
 */
public abstract class FileToFileProcessor extends AbstractFileProcessor {
    private static final String OUTPUT_ROOT = "output";
    private static final String METADATA_ROOT = "metadata";

    private Path srcRoot;
    private Path outputRoot;
    private Path metadataRoot;

    public FileToFileProcessor(ArrayBlockingQueue<Path> queue, Path srcRoot, Path targRoot) {
        super(queue);
        this.srcRoot = srcRoot.toAbsolutePath();
        this.outputRoot = targRoot.resolve(OUTPUT_ROOT);
        this.metadataRoot = targRoot.resolve(METADATA_ROOT);
    }

    @Override
    public void process(Path srcPath) throws IOException {
        String relPath = srcRoot.relativize(srcPath).toString();
        Path outputPath =  outputRoot.resolve(relPath+getExtension());
        Path metdataPath = metadataRoot.resolve(relPath+".json");
        process(relPath, srcPath, outputPath, metdataPath);
    }

    protected String getExtension() {
        return "";
    }
    protected abstract void process(String relPath, Path srcPath, Path outputPath, Path metadataPath) throws IOException;
}
