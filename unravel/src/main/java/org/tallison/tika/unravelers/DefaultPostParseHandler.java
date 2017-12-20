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

package org.tallison.tika.unravelers;

import org.apache.commons.lang.StringUtils;
import org.apache.tika.batch.fs.FSProperties;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.serialization.JsonMetadataList;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * NOT THREAD SAFE!
 */
public class DefaultPostParseHandler implements  PostParseHandler {

    private int filesHandled = 0;
    private final Path rootDir;
    private final String subDir;

    public DefaultPostParseHandler(Path rootDir) {
        this(rootDir, null);
    }

    public DefaultPostParseHandler(Path rootDir, String subDir) {
        this.rootDir = rootDir;
        this.subDir = subDir;
    }

    @Override
    public void handle(List<Metadata> metadataList) throws TikaException, IOException {
        String pathString = getPath(filesHandled);
        if (subDir != null) {
            pathString = subDir+"/"+pathString;
        }
        Path outPath = rootDir.resolve(pathString);
        String extractName = outPath.getFileName().toString().replaceAll(".json", "");
        metadataList.get(0).set("extract_path",
                (subDir == null) ? extractName :
                        subDir+"/"+extractName
                );
        metadataList.get(0).set(FSProperties.FS_REL_PATH, pathString.replaceAll(".json", ""));


        if (!Files.isDirectory(outPath.getParent())) {
            Files.createDirectories(outPath.getParent());
        }
        if (Files.isRegularFile(outPath)) {
            throw new FileAlreadyExistsException(outPath.toString());
        }

        try (Writer writer = Files.newBufferedWriter(outPath, StandardCharsets.UTF_8)) {
            JsonMetadataList.toJson(metadataList, writer);
            writer.flush();
        }
        filesHandled++;
    }

    static String getPath(int i) {
        if (i > 999999999) {
            throw new IllegalArgumentException("Can't have > 999,999,999 files");
        }
        String outputFileName = StringUtils.leftPad(Integer.toString(i), 9, '0');
        return outputFileName.substring(0,3)+"/"+outputFileName.substring(3,6)+"/"+outputFileName+".json";
    }
}
