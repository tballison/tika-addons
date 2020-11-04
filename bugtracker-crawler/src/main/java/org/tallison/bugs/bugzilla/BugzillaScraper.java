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
package org.tallison.bugs.bugzilla;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSyntaxException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.tallison.bugs.Attachment;
import org.tallison.bugs.ClientException;
import org.tallison.bugs.HttpUtils;
import org.tallison.bugs.ScraperUtils;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class BugzillaScraper {

    /**
     * https://infra.apache.org/infra-ban.html
     * BEWARE crawling apache resources.  Be kind. They aren't joking.
     */

/*
Thanks to @triagegirl for noting that bugzilla has an API!!!
 */
    /**
     * TODO: automatically figure out if the url needs .cgi?
     * TODO: consider gzipping .json issue files to save space
     */

    /*
        -p MOZILLA -u https://bugzilla.mozilla.org/ -m application -o /Users/allison/Desktop/mozilla -s 1000
        -p REDHAT -u https://bugzilla.redhat.com/ -m application -o /Users/allison/Desktop/redhat -s 1000
        -p OOO -u https://bz.apache.org/ooo -m application -o /docs/ooo -s 10
        -p POI -u https://bz.apache.org/bugzilla/ -m application -o /docs/poi -d POI -s 10
        -p LIBRE_OFFICE -u https://bugs.documentfoundation.org/ -m application -o /Users/allison/Desktop/libre -s 10
        -p GHOSTSCRIPT -u https://bugs.ghostscript.com/ -m application -o /Users/allison/Desktop/ghostscript -s 1000
        //this api has been shutdown
        //-p poppler -u https://bugs.freedesktop.org/ -m application -o /home/tallison/bugzilla -s 1000
     */


    private static int NUM_THREADS = 1;

    public static void main(String[] args) throws Exception {
        if (Files.isRegularFile(Paths.get(args[0]))) {
            processFile(Paths.get(args[0]));
        } else {
            processCommandline(args);
        }
    }

    private static void processCommandline(String[] args) throws Exception {
        ArrayBlockingQueue<String[]> commandLines = new ArrayBlockingQueue<>(2);
        commandLines.add(args);
        commandLines.add(BugzillaWorker.POISON_COMMANDLINE);
        BugzillaWorker worker = new BugzillaWorker(commandLines);
        worker.call();
    }

    private static void processFile(Path path) throws Exception {
        List<String[]> tmpCommandLines = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            String line = reader.readLine();
            while (line != null) {
                if (! line.startsWith("#")) {
                    tmpCommandLines.add(line.split(" "));
                }
                line = reader.readLine();
            }
        }

        ArrayBlockingQueue<String[]> commandLines = new ArrayBlockingQueue<>(tmpCommandLines.size()+NUM_THREADS);
        commandLines.addAll(tmpCommandLines);
        for (int i = 0; i < NUM_THREADS; i++) {
            commandLines.add(BugzillaWorker.POISON_COMMANDLINE);
        }

        ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
        ExecutorCompletionService executorCompletionService = new ExecutorCompletionService(executorService);

        for (int i = 0; i < NUM_THREADS; i++) {
            executorCompletionService.submit(new BugzillaWorker(commandLines));
        }
        int finished = 0;
        while (finished < NUM_THREADS) {
            Future<String> future = executorCompletionService.take();
            try {
                System.err.println("Finished: " + future.get());
            } catch (Exception e) {
                e.printStackTrace();
            }
            finished++;
            System.out.println("finished: "+finished);
        }
        executorService.shutdownNow();
    }
}
