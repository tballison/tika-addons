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
package org.tallison.langid.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.FileUtils;

public class SentenceSampler {
    Matcher langMatcher = Pattern.compile("^([-a-z]+)(_([a-z]+))?").matcher("");
    Random r = new Random();
    enum RAND_MODE {
        BTWN_0_1000000,
        PLUS_MINUS_1
    }
    private final RAND_MODE randMode;
    public SentenceSampler(RAND_MODE randMode) {
        this.randMode = randMode;
    }
    public static void main(String[] args) throws IOException {
        Path leipzig = Paths.get(args[0]);
        Path sampled = Paths.get(args[1]);
        RAND_MODE randMode = RAND_MODE.PLUS_MINUS_1;
        SentenceSampler sampler = new SentenceSampler(randMode);
        int length = 100000;
        double[] noiseLevels = new double[]{0.0, 0.05, 0.1, 0.2, 0.3, 0.5, 0.9};
        int numSamples = 50;

        sampler.sample(leipzig, sampled, numSamples, length, noiseLevels);
    }

    private void sample(Path leipzig, Path sampled, int numSamples, int length,
                        double[] noiseLevels) throws IOException {
        for (File f : leipzig.toFile().listFiles()) {
            try {
                processFile(f, sampled, numSamples, length, noiseLevels);
            } catch (Exception e) {
                e.printStackTrace();;
            }
        }
        dumpNumbers(sampled, length, noiseLevels);
    }

    private void dumpNumbers(Path sampled, int length, double[] noiseLevels) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            if (r.nextFloat() < 0.01) {
                sb.append(",");
            } else if (r.nextFloat() < 0.02) {
                sb.append(" ");
            } else {
                sb.append(r.nextInt());
            }
        }
        dumpAll("num_stuff", sb.toString(), sampled, noiseLevels, 0);
    }


    private void processFile(File f, Path sampledDir, int numSamples,
                             int length, double[] noiseLevels) throws IOException {
        if (f.isDirectory()) {
            return;
        }
        System.out.println("processing: " + f);
        List<String> rows = readLines(f);
        for (int i = 0; i < numSamples; i++) {
            Collections.shuffle(rows);

            StringBuilder bigString = new StringBuilder();

            for (String r : rows) {
                //trim sent number
                int index = r.indexOf("\t");
                bigString.append(r.substring(index + 1)).append(" ");
                if (bigString.length() > length) {
                    break;
                }
            }
            int end = Math.min(bigString.length(), length);
            String s = bigString.substring(0, end);
            dumpAll(f.getName(), s, sampledDir, noiseLevels, i);
        }
    }

    private List<String> readLines(File f) throws IOException{
        if (f.getName().endsWith(".gz")) {
            try (BufferedReader reader =
                    new BufferedReader(new InputStreamReader(
                            new GzipCompressorInputStream(
                                    Files.newInputStream(
                                            f.toPath())),StandardCharsets.UTF_8))) {
                String line = reader.readLine();
                List<String> lines = new ArrayList<>();
                while (line != null) {
                    lines.add(line);
                    line = reader.readLine();
                }
                return lines;
            }


        } else {
            return FileUtils.readLines(f, StandardCharsets.UTF_8);
        }
    }

    private void dumpAll(String fName, String s,
                         Path sampledDir,
                         double[] noiseLevels, int id)  throws IOException {

            for (double noise : noiseLevels) {

                StringBuilder sample = new StringBuilder();

                s.codePoints().forEach(c ->
                        sample.appendCodePoint(r.nextDouble() < noise ? randChar(c) : c)
                );

                String noiseString = Double.toString(noise);

                Path sampleSubDir = sampledDir.resolve(noiseString);

                langMatcher.reset(fName);
                String lang = null;
                String src = null;
                if (langMatcher.find()) {
                    lang = langMatcher.group(1);
                    src = langMatcher.group(2);
                } else {
                    throw new IllegalArgumentException(fName);
                }
                Path sampleFile = sampleSubDir.resolve(lang + "/"+lang+"_" + src+"_"+noiseString +
                        "_"+id+".txt.gz");
                if (! Files.isDirectory(sampleFile.getParent())) {
                    Files.createDirectories(sampleFile.getParent());
                }
                try (Writer writer = new OutputStreamWriter(
                        new GzipCompressorOutputStream(
                                Files.newOutputStream(sampleFile)),
                        StandardCharsets.UTF_8)) {
                    writer.write(sample.toString());
                }
            }


    }

    private int randChar(int c) {
        if (randMode.equals(RAND_MODE.BTWN_0_1000000)) {
            return r.nextInt(1000000);
        } else if (randMode.equals(RAND_MODE.PLUS_MINUS_1)) {
            if (r.nextFloat() < 0.5) {
                return c-1;
            } else {
                return c+1;
            }
        } else {
            throw new IllegalArgumentException("Don't yet support: "+randMode);
        }
    }
}
