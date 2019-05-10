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
    Matcher langMatcher = Pattern.compile("^([-a-z]+)_([a-z]+)").matcher("");
    Random r = new Random();
    public static void main(String[] args) throws IOException {
        Path leipzig = Paths.get(args[0]);
        Path sampled = Paths.get(args[1]);
        SentenceSampler sampler = new SentenceSampler();
        int[] lengths = new int[]{50, 100, 200, 500, 1000, 10000, 100000};
        double[] noiseLevels = new double[]{0.0, 0.05, 0.1, 0.2, 0.3, 0.5, 0.9};
        int numSamples = 50;
        sampler.sample(leipzig, sampled, numSamples, lengths, noiseLevels);
    }

    private void sample(Path leipzig, Path sampled, int numSamples, int[] lens,
                        double[] noiseLevels) throws IOException {
        for (File f : leipzig.toFile().listFiles()) {
            try {
                processFile(f, sampled, numSamples, lens, noiseLevels);
            } catch (Exception e) {
                e.printStackTrace();;
            }
        }
        dumpNumbers(sampled, lens, noiseLevels);
    }

    private void dumpNumbers(Path sampled, int[] lens, double[] noiseLevels) throws IOException {
        StringBuilder sb = new StringBuilder();
        int maxLen = getMax(lens);
        for (int i = 0; i < maxLen; i++) {
            if (r.nextFloat() < 0.01) {
                sb.append(",");
            } else if (r.nextFloat() < 0.02) {
                sb.append(" ");
            } else {
                sb.append(r.nextInt());
            }
        }
        dumpAll("num_stuff", sb, sampled, lens, noiseLevels, 0);
    }

    private static int getMax(int[] lens) {
        int maxLen = lens[0];
        for (int i : lens) {
            if (i > maxLen) {
                maxLen = i;
            }
        }
        return maxLen;
    }

    private void processFile(File f, Path sampledDir, int numSamples,
                             int[] lens, double[] noiseLevels) throws IOException {
        if (f.isDirectory()) {
            return;
        }
        List<String> rows = FileUtils.readLines(f, StandardCharsets.UTF_8);
        for (int i = 0; i < numSamples; i++) {
            Collections.shuffle(rows);

            StringBuilder bigString = new StringBuilder();
            int maxLen = getMax(lens);
            for (String r : rows) {
                //trim sent number
                int index = r.indexOf("\t");
                bigString.append(r.substring(index + 1)).append(" ");
                if (bigString.length() > maxLen) {
                    break;
                }
            }
            dumpAll(f.getName(), bigString, sampledDir, lens, noiseLevels, i);
        }
    }

    private void dumpAll(String fName, StringBuilder bigString, Path sampledDir, int[] lens,
                         double[] noiseLevels, int id)  throws IOException {
        for (int len : lens) {
            for (double noise : noiseLevels) {

                StringBuilder sample = new StringBuilder();

                bigString.codePoints().limit(len).forEach(c ->
                        sample.appendCodePoint(r.nextDouble() < noise ? randChar() : c)
                );

                String lenNoiseString = len + "_" + Double.toString(noise);

                Path sampleSubDir = sampledDir.resolve(lenNoiseString);

                langMatcher.reset(fName);
                String lang = null;
                String src = null;
                if (langMatcher.find()) {
                    lang = langMatcher.group(1);
                    src = langMatcher.group(2);
                } else {
                    throw new IllegalArgumentException(fName);
                }
                Path sampleFile = sampleSubDir.resolve(lang + "/"+lang+"_" + src+"_"+lenNoiseString +
                        "_"+id+".txt");
                if (! Files.isDirectory(sampleFile.getParent())) {
                    Files.createDirectories(sampleFile.getParent());
                }
                FileUtils.write(sampleFile.toFile(), sample.toString(), StandardCharsets.UTF_8);
            }
        }

    }

    private int randChar() {
        return r.nextInt(1000000);
    }
}
