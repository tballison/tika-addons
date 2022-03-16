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
package org.tallison.langid;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import org.tallison.langid.opennlp.OpenNLPLangDetector;

public class TrainTestSplit {

    private static String TRAIN = "train";
    private static String DEVTEST = "devtest";
    private static String TEST = "test";

    private double trainingPercentage = 0.7;
    private double devtestPercentage = 0.1;
    private double testPercentage = 0.2;
    private int sentsPerSample = 5;
    private int maxSamplesPerLanguage = 10000;

    Map<String, BufferedWriter> writers = new HashMap<>();
    //train devtest <lang, sampleCount>
    Map<String, Map<String, Integer>> samplesPerLang = new HashMap<>();

    Random r = new Random();

    public TrainTestSplit(Path outputDir) throws IOException {
        Files.createDirectories(outputDir);
        writers.put(TRAIN, Files.newBufferedWriter(
                outputDir.resolve(TRAIN + "_" + maxSamplesPerLanguage + ".txt"),
                StandardCharsets.UTF_8));
        writers.put(TEST, Files.newBufferedWriter(
                outputDir.resolve(TEST + "_" + maxSamplesPerLanguage + ".txt"),
                StandardCharsets.UTF_8));
        writers.put(DEVTEST, Files.newBufferedWriter(
                outputDir.resolve(DEVTEST + "_" + maxSamplesPerLanguage + ".txt"),
                StandardCharsets.UTF_8));
    }

    public static void main(String[] args) throws Exception {
        Path leipzigDir = Paths.get(args[0]);
        Path outputDir = Paths.get(args[1]);
        //Path langModel = Paths.get(args[2]);
        OpenNLPLangDetector ld = new OpenNLPLangDetector();
        TrainTestSplit split = new TrainTestSplit(outputDir);


        split.execute(ld, leipzigDir);
    }

    private void execute(OpenNLPLangDetector ld, Path leipzigDir) throws Exception {
        Map<String, List<File>> langs = new HashMap<>();
        for (File f : leipzigDir.toFile().listFiles()) {
            if (f.isDirectory()) {
                continue;
            }
            String n = f.getName();
            String lang = n.substring(0, 3);
            List<File> files = langs.get(lang);
            if (files == null) {
                files = new ArrayList<>();
            }

            files.add(f);
            langs.put(lang, files);
        }
        List<String> langKeys = new ArrayList<>(langs.keySet());
        Collections.sort(langKeys);
        for (String lang : langKeys) {
            processLang(ld, lang, langs.get(lang));
        }
        for (Writer w : writers.values()) {
            w.flush();
            w.close();
        }
    }

    private void processLang(OpenNLPLangDetector ld, String lang, List<File> files)
            throws Exception {
        if (seenEnough(lang)) {
            return;
        }
        List<String> lines = new ArrayList<>();
        for (File f : files) {
            lines.addAll(FileUtils.readLines(f, StandardCharsets.UTF_8));
        }
        Collections.shuffle(lines);
        System.out.println(lang + " " + lines.size());
        int added = 0;
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i);
            line = line.substring(line.indexOf("\t") + 1);
            if (line.length() > 50) {
                sb.append(" ");
                sb.append(line);
                added++;
            }
            if (added == sentsPerSample) {
                writeSample(lang, sb.toString());
                added = 0;
                sb.setLength(0);
            }
            if (seenEnough(lang)) {
                return;
            }
        }
        writeSample(lang, sb.toString());
    }

    private void writeSample(String lang, String sample) throws IOException {

        float val = r.nextFloat();

        sample = prepSample(lang, sample);
        if (val < trainingPercentage) {
            if (seenEnough(TRAIN, lang)) {
//                System.err.println("seen enough " + TRAIN + " " + lang);
                return;
            }
            writers.get(TRAIN).write(sample);
            increment(TRAIN, lang);
        } else if (val < trainingPercentage + devtestPercentage) {
            if (seenEnough(DEVTEST, lang)) {
  //              System.err.println("seen enough " + DEVTEST + " " + lang);
                return;
            }
            writers.get(DEVTEST).write(sample);
            increment(DEVTEST, lang);
        } else {
            if (seenEnough(TEST, lang)) {
    //            System.err.println("seen enough " + TEST + " " + lang);
                return;
            }
            writers.get(TEST).write(sample);
            increment(TEST, lang);
        }
    }

    private void increment(String split, String lang) {
        Map<String, Integer> counts = samplesPerLang.get(split);
        if (counts == null) {
            counts = new HashMap<>();
            samplesPerLang.put(split, counts);
        }
        Integer sampleCount = counts.get(lang);
        if (sampleCount == null) {
            counts.put(lang, 1);
        } else {
            counts.put(lang, sampleCount + 1);
        }
    }

    private boolean seenEnough(String lang) {
        return seenEnough(TRAIN, lang) && seenEnough(TEST, lang) && seenEnough(DEVTEST, lang);
    }

    private boolean seenEnough(String split, String lang) {
        Map<String, Integer> counts = samplesPerLang.get(split);
        if (counts == null) {
            return false;
        }
        Integer cnt = counts.get(lang);
        if (cnt == null) {
            return false;
        }
        if (cnt > maxSamplesPerLanguage) {
            return true;
        }
        return false;
    }

    private String prepSample(String lang, String sample) {
        sample = sample.replaceAll("\r\n\t", " ");
        return lang + "\t" + sample + "\n";
    }
}
