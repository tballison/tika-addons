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
package org.tallison.bugs;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

/**
 * take files out of nested directories and drop
 * them into a single directory
 */
public class DirectoryFlattener {

    public static void main(String[] args) throws Exception {
        Path srcRoot = Paths.get(args[0]);
        Path targRoot = Paths.get(args[1]);
        Files.createDirectories(targRoot);
        process(srcRoot, targRoot);
    }

    private static void process(Path path, Path targRoot) throws IOException {
        if (Files.isDirectory(path)) {
            handleDirectory(path, targRoot);
        } else {
            handleFile(path, targRoot);
        }
    }

    private static void handleFile(Path path, Path targRoot) throws IOException {
        Path targFile = targRoot.resolve(path.getFileName().toString());
        if (Files.exists(targFile)) {
            System.err.println("skipping "+targFile +
                    " coz it exists!");
            return;
        } else {
            System.out.println("mv'ing "+path.toAbsolutePath().toString() +
                    " -> "+targFile.toAbsolutePath().toString());

            Files.move(path, targFile, StandardCopyOption.ATOMIC_MOVE);
        }
    }

    private static void handleDirectory(Path path, Path targRoot) throws IOException {
        for (File f : path.toFile().listFiles()) {
            process(f.toPath(), targRoot);
        }
    }
}
