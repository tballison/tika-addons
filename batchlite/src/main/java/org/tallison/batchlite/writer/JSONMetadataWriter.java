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

import com.google.gson.Gson;
import org.tallison.batchlite.FileProcessResult;
import org.tallison.batchlite.MetadataWriter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

public class JSONMetadataWriter implements MetadataWriter {

    private final static Gson GSON = new Gson();

    private final Path metadataRootDir;
    private AtomicInteger recordsWritten = new AtomicInteger(0);

    public JSONMetadataWriter(Path metadataRootDir) {
        this.metadataRootDir = metadataRootDir;
    }

    @Override
    public void write(String relPath, FileProcessResult result) throws IOException {
        Path target = metadataRootDir.resolve(relPath + ".json");
        if (! Files.isDirectory(target.getParent())) {
            Files.createDirectories(target.getParent());
        }
        Files.write(target, GSON.toJson(result).getBytes(StandardCharsets.UTF_8));
        recordsWritten.incrementAndGet();
    }

    @Override
    public int getRecordsWritten() {
        return recordsWritten.get();
    }

    @Override
    public void close() throws IOException {

    }
}
