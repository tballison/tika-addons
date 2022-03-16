package org.tallison.langid.tools;

import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

/**
 * OpenNLP's big confusion matrix is too hard to read w > 100
 * langs.  This does a pairwise confusion matrix, which is
 * easier to read.
 */
public class OpenNLPMisclassifiedToPairwiseConfusion {

    public static void main(String[] args) throws Exception {
        Path path = Paths.get(args[0]);
        Map<String, Integer> confusions = new HashMap();
        try (BufferedReader r = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            String line = r.readLine();
            while (line != null) {
                String[] cols = line.split("\t");
                if (cols.length != 3) {
                    line = r.readLine();
                    continue;
                }
                String[] langs = new String[2];
                langs[0] = cols[0];
                langs[1] = cols[1];
                Arrays.sort(langs);
                String confusion = langs[0]+"<->"+langs[1];
                Integer cnt = confusions.get(confusion);
                if (cnt == null) {
                    confusions.put(confusion, 1);
                } else {
                    confusions.put(confusion, cnt+1);
                }
                line = r.readLine();
            }
        }
        Stream<Map.Entry<String,Integer>> sorted =
                confusions.entrySet().stream()
                        .sorted(Map.Entry.comparingByValue());
        sorted.forEach( e -> System.out.println(e.getKey() + "\t"+e.getValue()));
    }
}
