package org.tallison.tika.parser.forkrecursive;/*
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

import org.apache.commons.io.FileUtils;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.serialization.JsonMetadataList;
import org.apache.tika.parser.RecursiveParserWrapper;
import org.junit.Ignore;
import org.tallison.tika.parser.forkrecursive.RecursiveForkParser;
import org.tallison.tika.parser.forkrecursive.TikaChildProcess;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RecursiveForkParserTest {

    private static final Path POISON = Paths.get("");

    static RecursiveForkParser FORK_PARSER;

    @BeforeClass
    public static void setUpClass() {
        FORK_PARSER = new RecursiveForkParser(getDefaultParameters());
    }

    static List<String> getDefaultParameters() {
        List<String> command = new ArrayList<>();
        command.add("java");
        String classPath = System.getProperty("java.class.path");
        command.add("-cp");
        command.add(classPath);
        command.add("-D"+TikaChildProcess.TIKA_FORKED_CHILD_TIMEOUT_PROP+"=10000");
        command.add("-Dlog4j.configuration=child-log4j.properties");
        return command;
    }

    @AfterClass
    public static void tearDownClass() {
        FORK_PARSER.close();
    }

    @Test
    public void testBasic() throws Exception {
        Path tmpOut = null;
        try {
            tmpOut = Files.createTempDirectory("tika-batch");
            executeBatch(FORK_PARSER, "basic", 10, tmpOut);
            File[] files = tmpOut.toFile().listFiles();
            assertEquals(9, files.length);
            for (File f : files) {
                if (f.getName().contains("test")) {
                    assertTrue(f.getName(), f.exists());
                    List<Metadata> metadataList = null;
                    try (Reader r = Files.newBufferedReader(f.toPath(), StandardCharsets.UTF_8)) {
                        metadataList = JsonMetadataList.fromJson(r);
                    }
                    assertNotNull(metadataList);
                    assertEquals(1, metadataList.size());
                    assertEquals("Nikolai Lobachevsky", metadataList.get(0).get("author"));
                    assertTrue(metadataList.get(0).get(RecursiveParserWrapper.TIKA_CONTENT).contains("This is tika-batch"));
                }
            }
        } finally {
            FileUtils.deleteDirectory(tmpOut.toFile());
        }
    }

    @Test
    public void testLong() throws Exception {
        //there's a limit to how long an object can be with writeUTF()
        //confirm that we aren't using that any more!
        Path tmpOut = null;
        try {
            tmpOut = Files.createTempDirectory("tika-batch");
            executeBatch(FORK_PARSER, "long", 10, tmpOut);
            File[] files = tmpOut.toFile().listFiles();
            assertEquals(1, files.length);
            for (File f : files) {
                if (f.getName().contains("test")) {
                    assertTrue(f.getName(), f.exists());
                    List<Metadata> metadataList = null;
                    try (Reader r = Files.newBufferedReader(f.toPath(), StandardCharsets.UTF_8)) {
                        metadataList = JsonMetadataList.fromJson(r);
                    }
                    assertNotNull(metadataList);
                    assertEquals(1, metadataList.size());
                    assertTrue(metadataList.get(0).get(RecursiveParserWrapper.TIKA_CONTENT).contains("1234567890"));
                }
            }
        } finally {
            FileUtils.deleteDirectory(tmpOut.toFile());
        }
    }

    @Test
    public void testZh() throws Exception {
        Path tmpOut = null;
        try {
            tmpOut = Files.createTempDirectory("tika-batch");
            executeBatch(FORK_PARSER, "zh", 1, tmpOut);
            File[] files = tmpOut.toFile().listFiles();
            assertEquals(1, files.length);
            for (File f : files) {
                if (f.getName().contains("test")) {
                    assertTrue(f.getName(), f.exists());
                    List<Metadata> metadataList = null;
                    try (Reader r = Files.newBufferedReader(f.toPath(), StandardCharsets.UTF_8)) {
                        metadataList = JsonMetadataList.fromJson(r);
                    }
                    assertNotNull(metadataList);
                    assertEquals(1, metadataList.size());
                    assertTrue(metadataList.get(0).get(RecursiveParserWrapper.TIKA_CONTENT).contains("普林斯顿大学"));
                }
            }
        } finally {
            FileUtils.deleteDirectory(tmpOut.toFile());
        }
    }

    @Test(timeout = 20000)
    public void testSleepyTimeout() throws Exception {
        List<String> command = getDefaultParameters();
        command.add("-D"+TikaChildProcess.TIKA_FORKED_CHILD_TIMEOUT_PROP+"=1000");
        RecursiveForkParser shortFuseParser = new RecursiveForkParser(command);
        Path tmpOut = null;
        try {
            tmpOut = Files.createTempDirectory("tika-batch");
            executeBatch(shortFuseParser, "sleep", 2, tmpOut);
            File[] files = tmpOut.toFile().listFiles();
            assertEquals(3, files.length);
            for (File f : files) {
                assertEquals(0, f.length());
            }
        } finally {
            FileUtils.deleteDirectory(tmpOut.toFile());
        }
    }
    @Test
    public void testOOM() throws Exception {
        Path tmpOut = null;
        List<String> command = getDefaultParameters();
        command.add("-Xmx64m");
        try {
            tmpOut = Files.createTempDirectory("tika-batch");
            executeBatch(new RecursiveForkParser(command),"oom", 3, tmpOut);
            File[] files = tmpOut.toFile().listFiles();
            assertEquals(6, files.length);
            for (File f : files) {
                if (f.getName().contains("heavy_hang") || f.getName().contains("oom")) {
                    assertTrue(f.getName(), f.isFile());
                    assertEquals(f.getName(), 0, f.length());
                } else {
                    assertTrue(f.getName(), f.exists());
                    List<Metadata> metadataList = null;
                    try (Reader r = Files.newBufferedReader(f.toPath(), StandardCharsets.UTF_8)) {
                        metadataList =JsonMetadataList.fromJson(r);
                    }
                    assertNotNull(metadataList);
                    assertEquals(1, metadataList.size());
                    assertEquals("Nikolai Lobachevsky", metadataList.get(0).get("author"));
                    assertTrue(metadataList.get(0).get(RecursiveParserWrapper.TIKA_CONTENT).contains("This is tika-batch"));
                }
            }
        } finally {
            FileUtils.deleteDirectory(tmpOut.toFile());
        }
    }

    @Test
    public void testNoisy() throws Exception {
        Path tmpOut = null;
        try {
            tmpOut = Files.createTempDirectory("tika-batch");
            executeBatch(FORK_PARSER, "noisy_parsers", 3, tmpOut);
            File[] files = tmpOut.toFile().listFiles();
            assertEquals(2, files.length);
            for (File f : files) {
                    assertTrue(f.getName(), f.exists());
                    List<Metadata> metadataList = null;
                    try (Reader r = Files.newBufferedReader(f.toPath(), StandardCharsets.UTF_8)) {
                        metadataList =JsonMetadataList.fromJson(r);
                    }
                    assertNotNull(metadataList);
                    assertEquals(1, metadataList.size());
                    assertEquals("Nikolai Lobachevsky", metadataList.get(0).get("author"));
                    assertTrue(metadataList.get(0).get(RecursiveParserWrapper.TIKA_CONTENT).contains("This is tika-batch"));
            }
        } finally {
            FileUtils.deleteDirectory(tmpOut.toFile());
        }
    }

    @Test
    public void testHeavyHangs() throws Exception {
        Path tmpOut = null;
        try {
            tmpOut = Files.createTempDirectory("tika-batch");
            executeBatch(FORK_PARSER, "heavy_heavy_hangs", 2, tmpOut);
            File[] files = tmpOut.toFile().listFiles();
            for (File f : files) {
                if (f.getName().contains("heavy_hang")) {
                    assertTrue(f.getName(), f.isFile());
                    assertEquals(f.getName(), 0, f.length());
                } else {
                    assertTrue(f.getName(), f.exists());
                    List<Metadata> metadataList = null;
                    try (Reader r = Files.newBufferedReader(f.toPath(), StandardCharsets.UTF_8)) {
                        metadataList =JsonMetadataList.fromJson(r);
                    }
                    assertNotNull(metadataList);
                    assertEquals(1, metadataList.size());
                    assertEquals("Nikolai Lobachevsky", metadataList.get(0).get("author"));
                    assertTrue(metadataList.get(0).get(RecursiveParserWrapper.TIKA_CONTENT).contains("This is tika-batch"));
                }
            }
        } finally {
            FileUtils.deleteDirectory(tmpOut.toFile());
        }
    }

    private void executeBatch(RecursiveForkParser parser, String dirName, int numThreads, Path outputDir) throws Exception {
        ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        ExecutorCompletionService<Integer> executorCompletionService = new ExecutorCompletionService<>(executorService);
        ArrayBlockingQueue<Path> queue = new ArrayBlockingQueue<>(1000);
        for (File f : Paths.get(
                this.getClass().getResource("/"+dirName).toURI()).toFile().listFiles()) {
            queue.put(f.toPath());
        }
        for (int i = 0; i < numThreads; i++) {
            queue.put(POISON);
        }
        for (int i = 0; i < numThreads; i++) {
            executorCompletionService.submit(new TikaThread(parser, queue, outputDir));
        }
        int finished = 0;
        while (true) {
            Future<Integer> future = executorCompletionService.poll(1, TimeUnit.SECONDS);
            if (future != null) {
                finished++;
                Integer completed = future.get();
            }
            if (finished >= numThreads) {
                break;
            }
        }
    }

    private class TikaThread implements Callable<Integer> {
        private final RecursiveForkParser parser;
        private final ArrayBlockingQueue<Path> queue;
        private final Path tmpOut;
        TikaThread(RecursiveForkParser parser, ArrayBlockingQueue<Path> queue, Path tmpOut) {
            this.parser = parser;
            this.queue = queue;
            this.tmpOut = tmpOut;
        }
        @Override
        public Integer call() {
            while (true) {
                Path p = null;
                try {
                    p = queue.poll(1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    return 0;
                }
                if (p == null) {
                    continue;
                }
                if (p.equals(POISON)) {
                    return 0;
                }

                List<Metadata> metadataList = null;
                try {
                     metadataList = parser.parse(p);
                } catch (TikaException e) {
                    e.printStackTrace();
                }
                try (Writer writer = Files.newBufferedWriter(tmpOut.resolve(p.getFileName()), StandardCharsets.UTF_8)) {
                    if (metadataList != null) {
                        JsonMetadataList.toJson(metadataList, writer);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (TikaException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
