package org.tallison.tika.eval;


import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.tika.eval.tokens.CommonTokenCountManager;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class MultiComparer {

    public static void main(String[] args) throws IOException {
        Path reportDir = Paths.get(args[0]);
        File[] tools = new File(args[1]).listFiles();
        Arrays.sort(tools);
        List<Path> dirs = new ArrayList<>();
        for (File dir : tools) {
            dirs.add(dir.toPath());
        }

        MultiComparer comparer = new MultiComparer();
        comparer.execute(reportDir, dirs);
    }

    private void execute(Path reportDir, List<Path> dirs) throws IOException {
        Set<String> relPathUnion = getPaths(dirs);
        Map<String, ComparisonRecord> records = new TreeMap<>();
        int cnt = 0;
        for (String relPath : relPathUnion) {
            multiCompare(relPath, dirs, records);
            if (++cnt % 100 == 0) {
                System.err.println("processed "+cnt + " files out of "+relPathUnion.size());
            }
        }
        dumpResults(reportDir, records);
    }

    private void dumpResults(Path reportDir, Map<String, ComparisonRecord> records) throws IOException {
        Files.createDirectories(reportDir);
        for (String report : new String[]{"fileLengths", "simSets", "wordCounts"}) {
            writeReport(reportDir, report, records);
        }
    }

    private void writeReport(Path reportDir, String report, Map<String, ComparisonRecord> records) throws IOException {
        boolean header = false;
        try (BufferedWriter writer = Files.newBufferedWriter(reportDir.resolve(report+".txt"), StandardCharsets.UTF_8)) {
            for (String k : records.keySet()) {
                if (! header) {
                    writer.write(joinWith("\t", "file", records.get(k).dirRoots));
                    if (report.startsWith("simSets")) {
                        writer.write("\tTotalSets");
                    }
                    writer.newLine();
                    header = true;
                }
                if (report.startsWith("file")) {
                    writer.write(joinWith("\t", k, records.get(k).fileLength));
                } else if (report.startsWith("simSets")) {
                    writer.write(joinWith("\t", k, records.get(k).simSetValues));
                } else if (report.startsWith("wordCounts")) {
                    writer.write(joinWith("\t", k, records.get(k).numWords));
                }
                writer.newLine();

            }
        }
    }

    private String joinWith(String delimter, String id, List<String> strings) {
        StringBuilder sb = new StringBuilder();
        sb.append(id);
        for (String s : strings) {
                sb.append(delimter);
            sb.append(s);
        }
        return sb.toString();
    }

    private void multiCompare(String relPath, List<Path> dirs, Map<String, ComparisonRecord> records) throws IOException {

        ComparisonRecord record = new ComparisonRecord();
        List<Map<String, Integer>> wordSets = new ArrayList<>();
        for (Path dir : dirs) {
            record.dirRoots.add(dir.getFileName().toString());
            processPath(dir.resolve(relPath), wordSets, record);
        }
        for (Path d : dirs) {
            record.simSetValues.add("");
        }
        int setId = 0;
        String setIdString = "set_"+setId;
        record.simSetValues.set(0, setIdString);
        for (int i = 0; i  < wordSets.size()-1; i++) {
            if (record.simSetValues.get(i).equals("")) {
                setIdString = "set_"+ ++setId;
                record.simSetValues.set(i, setIdString);
            }
            for (int j = i+1; j < wordSets.size(); j++) {
                //if this has already been tagged, don't bother comparing it
                if (! record.simSetValues.get(j).equals("")) {
                    continue;
                }
                if (equals(wordSets.get(i), wordSets.get(j))) {
                    record.simSetValues.set(j, record.simSetValues.get(i));
                }
            }
        }
        if (record.simSetValues.get(wordSets.size()-1).equals("")) {
            record.simSetValues.set(wordSets.size()-1, "set_"+ ++setId);
        }
        record.simSetValues.add(Integer.toString(++setId));
        records.put(relPath, record);
    }

    private boolean equals(Map<String, Integer> mapA, Map<String, Integer> mapB) {
        if (mapA.size() != mapB.size()) {
            return false;
        }
        for (String k : mapA.keySet()) {
            if (! mapB.containsKey(k)) {
                return false;
            }
            if (! mapA.get(k).equals(mapB.get(k))) {
                return false;
            }
        }
        return true;
    }

    private void processPath(Path path, List<Map<String, Integer>> words, ComparisonRecord record) throws IOException {
        if (! Files.exists(path)) {
            handleMissing(words, record);
            return;
        }
        record.fileLength.add(Long.toString(Files.size(path)));
        String txt = FileUtils.readFileToString(path.toFile(), StandardCharsets.UTF_8);
        Analyzer analyzer = new StandardAnalyzer();
        TokenStream ts = analyzer.tokenStream("", txt);
        ts.reset();
        Map<String, Integer> counts = new HashMap<>();
        CharTermAttribute charTermAttribute = ts.addAttribute(CharTermAttribute.class);

        Iterator<Integer> cpIt = txt.codePoints().iterator();
        int alphabetic = 0;
        int nonWhitespace = 0;
        while (cpIt.hasNext()) {
            int cp = cpIt.next();
            if (Character.isAlphabetic(cp)) {
                alphabetic++;
            }
            if (! Character.isWhitespace(cp)) {
                nonWhitespace++;
            }
        }
        int wordCount = 0;
        while (ts.incrementToken()) {
            String token = charTermAttribute.toString();
            Integer i = counts.get(token);
            if (i == null) {
                i = 1;
            } else {
                i++;
            }
            counts.put(token, i);
            wordCount++;
        }
        words.add(counts);
        record.numAlphabeticChars.add(Integer.toString(alphabetic));
        record.numWords.add(Integer.toString(wordCount));
        record.numNotWhiteSpace.add(Integer.toString(nonWhitespace));
    }

    private void handleMissing(List<Map<String, Integer>> words, ComparisonRecord record) {
        words.add(new HashMap<>());
        record.fileLength.add("-1");
        record.numAlphabeticChars.add("-1");
        record.numNotWhiteSpace.add("-1");
        record.numWords.add("-1");
    }

    private Set<String> getPaths(List<Path> dirs) {
        Set<String> relPaths = new HashSet<>();
        for (Path dir : dirs) {
            addPaths(dir, relPaths);
        }
        return relPaths;
    }

    private void addPaths(Path dir, Set<String> relPaths) {
        processDirectory(dir, dir, relPaths);
    }

    private void processDirectory(Path rootDir, Path dir, Set<String> relPaths) {

        for (File f : dir.toFile().listFiles()) {
            if (f.isDirectory()) {
                processDirectory(rootDir, f.toPath(), relPaths);
            } else {
                String relPath = rootDir.relativize(f.toPath()).toString();
                if (relPath.contains("DS_Store")) {
                    continue;
                }
                relPaths.add(relPath);
            }
        }
    }

    private class ComparisonRecord {
        List<String> dirRoots = new ArrayList<>();
        List<String> simSetValues = new ArrayList<>();
        List<String> numWords = new ArrayList<>();
        List<String> numAlphabeticChars = new ArrayList<>();
        List<String> numNotWhiteSpace = new ArrayList<>();
        List<String> fileLength = new ArrayList<>();
    }
}
