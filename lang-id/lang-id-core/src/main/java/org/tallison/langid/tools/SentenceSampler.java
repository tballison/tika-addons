package org.tallison.langid.tools;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;

public class SentenceSampler {
    Matcher langMatcher = Pattern.compile("^([-a-z]+)_").matcher("");
    Random r = new Random();
    public static void main(String[] args) throws IOException {
        Path leipzig = Paths.get(args[0]);
        Path sampled = Paths.get(args[1]);
        SentenceSampler sampler = new SentenceSampler();
        int[] lengths = new int[]{50, 100, 200, 500, 1000, 10000, 100000};
        double[] noiseLevels = new double[]{0.0, 0.05, 0.1, 0.2, 0.3, 0.5, 0.9};
        sampler.sample(leipzig, sampled, lengths, noiseLevels);
    }

    private void sample(Path leipzig, Path sampled, int[] lens,
                        double[] noiseLevels) throws IOException {
        for (File f : leipzig.toFile().listFiles()) {
            try {
                processFile(f, sampled, lens, noiseLevels);
            } catch (Exception e) {
                e.printStackTrace();;
            }
        }
        dumpNumbers(sampled, lens, noiseLevels);
    }

    private void dumpNumbers(Path sampled, int[] lens, double[] noiseLevels) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 200000; i++) {
            if (r.nextFloat() < 0.01) {
                sb.append(",");
            } else if (r.nextFloat() < 0.02) {
                sb.append(" ");
            } else {
                sb.append(r.nextInt());
            }
        }
        dumpAll("num_stuff", sb, sampled, lens, noiseLevels);
    }

    private void processFile(File f, Path sampledDir, int[] lens, double[] noiseLevels) throws IOException {
        if (f.isDirectory()) {
            return;
        }
        List<String> rows = FileUtils.readLines(f, StandardCharsets.UTF_8);
        Collections.shuffle(rows);
        StringBuilder bigString = new StringBuilder();
        for (String r : rows) {
            //trim sent number
            int i = r.indexOf("\t");
            bigString.append(r.substring(i+1)).append(" ");
        }
        rows.clear();
        dumpAll(f.getName(), bigString, sampledDir, lens, noiseLevels);
    }

    private void dumpAll(String fName, StringBuilder bigString, Path sampledDir, int[] lens,
                         double[] noiseLevels)  throws IOException {
        for (int len : lens) {
            for (double noise : noiseLevels) {

                StringBuilder sample = new StringBuilder();
                int i = 0;
                bigString.codePoints().limit(len).forEach(c ->
                        sample.appendCodePoint(r.nextDouble() < noise ? randChar() : c)
                );

                String lenNoiseString = len + "_" + Double.toString(noise).replaceAll("\\.", "_");
                Path sampleSubDir = sampledDir.resolve(lenNoiseString);
                Files.createDirectories(sampleSubDir);

                langMatcher.reset(fName);
                String lang = null;
                if (langMatcher.find()) {
                    lang = langMatcher.group(1);
                } else {
                    throw new IllegalArgumentException(fName);
                }
                Path sampleFile = sampleSubDir.resolve(lang + "_" + lenNoiseString + ".txt");
                FileUtils.write(sampleFile.toFile(), sample.toString(), StandardCharsets.UTF_8);
            }
        }

    }

    private int randChar() {
        return r.nextInt(1000000);
    }
}
