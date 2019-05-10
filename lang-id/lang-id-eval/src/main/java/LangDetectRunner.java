import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.commons.math3.stat.descriptive.rank.Median;
import org.tallison.langid.LangDetectResult;
import org.tallison.langid.LangDetector;
import org.tallison.langid.opennlp.OpenNLPLangDetector;
import org.tallison.langid.optimaize.OptimaizeLangDetector;
import org.tallison.langid.yalder.YalderDetector;

public class LangDetectRunner {

    Matcher m = Pattern.compile("(([a-z]+)(?:-[a-z]+)?)_(\\w+)_(\\d+)_0\\.(\\d+)_(\\d+)").matcher("");
    //detector, length, processing time
    Map<String, Map<Integer, List<Long>>> processingTimes = new HashMap<>();
    DecimalFormat df = new DecimalFormat("#.##");
    DecimalFormat confidenceFormat = new DecimalFormat("#.####");


    private final Writer writer;

    public static void main(String[] args) throws Exception {
        Path sampleDir = Paths.get(args[0]);
        try (
                BufferedWriter fullTableWriter = Files.newBufferedWriter(Paths.get(args[1]),
                        StandardCharsets.UTF_8);
                BufferedWriter aggregatedResultsWriter = Files.newBufferedWriter(Paths.get(args[2]),
                        StandardCharsets.UTF_8)) {

            LangDetector[] detectors = new LangDetector[]{
                    new YalderDetector(),
                    new OptimaizeLangDetector(),
                    new OpenNLPLangDetector()
            };
            LangDetectRunner runner = new LangDetectRunner(fullTableWriter);
            List<Result> results = new ArrayList<>();
            runner.execute(sampleDir, detectors, results);
            runner.dumpResults(detectors, results, aggregatedResultsWriter);
        }

    }

    public LangDetectRunner(Writer writer) throws Exception {
        this.writer = writer;
        writer.write(StringUtils.joinWith("\t",
                "detector",
                "sampleFile",
                "fullLang",
                "expectedLang",
                "src",
                "hit",
                "length",
                "noise",
                "id",
                "lang1",
                "lang1Conf",
                "lang2",
                "lang2Conf",
                "elapsed(ms)") + "\n");
    }

    private void execute(Path sampleDir, LangDetector[] detectorArr, List<Result> results) throws IOException {
        List<LangDetector> detectors = Arrays.asList(detectorArr);
        for (File subdir : sampleDir.toFile().listFiles()) {
            for (File langdir : subdir.listFiles()) {
                for (File sampleFile : langdir.listFiles()) {
                    System.err.println("processing: " +langdir + " : "+ sampleFile);
                    String string = FileUtils.readFileToString(sampleFile, StandardCharsets.UTF_8);
                    for (LangDetector detector : detectors) {
                        try {
                            results.add(processSample(sampleFile, string, detector));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    Collections.shuffle(detectors);
                }
            }
        }
    }

    private void dumpResults(LangDetector[] detectorArr,
                             List<Result> results, BufferedWriter writer) throws IOException {
        Set<Integer> lengthSet = new LinkedHashSet<>();
        Set<String> noise = new LinkedHashSet<>();
        Set<String> detectors = new LinkedHashSet<>();
        Set<String> langs = new LinkedHashSet<>();
        int maxDetectorNameLength = -1;
        for (Result r : results) {
            lengthSet.add(r.length);
            noise.add(r.noise);
            detectors.add(r.detector);
            langs.add(r.expectedlang);
            if (r.detector.length() > maxDetectorNameLength) {
                maxDetectorNameLength = r.detector.length();
            }
        }
        maxDetectorNameLength += 5;
        List<Integer> lengths = new ArrayList<>(lengthSet);
        Collections.sort(lengths);
        writer.write("\nCovered Languages");
        writer.newLine();
        for (LangDetector d : detectorArr) {
            int covered = 0;
            for (String l : langs) {
                if (d.getSupportedLangs().contains(l)) {
                    covered++;
                }
            }
            //substract 1 for "num"
            writer.write("DETECTOR: " + d.getClass().getSimpleName() +
                    ("(" + covered + " out of " + (langs.size()-1) + ")"));
            writer.newLine();
            for (String l : langs) {
                writer.write("\t" + l + "\t" + d.getSupportedLangs().contains(l));
                writer.newLine();
            }
        }
        writer.newLine();
        writer.write("\nTime in Millis");
        writer.newLine();
        writer.write(
                "Detector\tLength\tMillis\tAvg(ms)\tStdev"
        );
        writer.newLine();
        for (String d : detectors) {
            for (Integer length : lengths) {
                List<Long> millis = processingTimes.get(d).get(length);
                dump(d, length, millis, writer);
            }
        }

        writer.newLine();

        writer.write("\nAccuracy Per Languages -- Detector/Length/Noise/Language");
        writer.newLine();
        for (String d : detectors) {
            writer.write("DETECTOR: " + d);
            writer.newLine();
            for (Integer len : lengths) {
                writer.write("\tLENGTH: " + len);
                writer.newLine();
                for (String n : noise) {
                    writer.write("\t\tNOISE: " + denoise(n));
                    writer.newLine();
                    for (String lang : langs) {
                        double accuracy = calcAccuracy(d, len, n, lang, results);
                        if (accuracy >= 0.0) {
                            writer.write(
                                    StringUtils.joinWith(" ", "\t\t\t",
                                            d, "len=" + len, "noise=" + denoise(n), "lang=" + lang,
                                            "accuracy=" + df.format(accuracy))
                            );
                            writer.newLine();
                        }
                    }
                }
            }
        }

        writer.newLine();
        writer.write("Accuracy Per Languages -- Length/Noise/Language/Detector");
        writer.newLine();
        for (Integer len : lengths) {
            writer.write("\tLENGTH: " + len);
            writer.newLine();
            for (String n : noise) {
                writer.write("\t\tNOISE: " + denoise(n));
                writer.newLine();
                for (String lang : langs) {
                    for (String d : detectors) {
                        double accuracy = calcAccuracy(d, len, n, lang, results);
                        if (accuracy >= 0.0) {
                            writer.write(
                                    StringUtils.joinWith(" ", "\t\t\t",
                                            StringUtils.rightPad(d, maxDetectorNameLength, " "), "len=" + len, "noise=" + denoise(n), "lang=" + lang,
                                            "accuracy=" + df.format(accuracy))
                            );
                            writer.newLine();
                        }
                    }
                }
            }
        }

        writer.newLine();
        writer.write("Accuracy Across Languages -- Detector/Length/Noise");
        writer.newLine();
        for (String d : detectors) {
            writer.write("DETECTOR: " + d);
            writer.newLine();
            for (Integer len : lengths) {
                writer.write("\tLENGTH: " + len);
                writer.newLine();
                for (String n : noise) {
                    double accuracy = calcOverallAccuracy(d, len, n, results);
                    writer.write(
                            StringUtils.joinWith(" ", "\t\t\t",
                                    StringUtils.rightPad(d, maxDetectorNameLength, " "), "len=" + len, "noise=" + denoise(n),
                                    "accuracy=" + df.format(accuracy))
                    );
                    writer.newLine();
                }
            }
        }
        writer.newLine();
        writer.write("CONFIDENCE SCORES -- Detector/Length/Noise");
        writer.newLine();
        for (String d : detectors) {
            writer.write("DETECTOR: " + d);
            writer.newLine();
            for (Integer len : lengths) {
                writer.write("\tLENGTH: " + len);
                writer.newLine();
                for (String n : noise) {
                    SummaryStatistics sm = new SummaryStatistics();
                    double median = calcConfidence(d, len, n, results, sm);
                    writer.write(
                            StringUtils.joinWith(" ", "\t\t\t",
                                    StringUtils.rightPad(d, maxDetectorNameLength, " "), "len=" + len, "noise=" + denoise(n),
                                    "confidence_mean=" + df.format(sm.getMean()),
                                    "confidence_stdev=" + df.format(sm.getStandardDeviation()),
                                    "confidence_median=" + df.format(median)
                            ));
                    writer.newLine();
                }
            }
        }
        writer.newLine();
        writer.write("Accuracy Across Languages -- Detector/Noise/Length");
        writer.newLine();
        for (String d : detectors) {
            writer.write("DETECTOR: " + d);
            writer.newLine();
            for (String n : noise) {
                writer.write("\tNOISE: " + denoise(n));
                writer.newLine();
                for (Integer len : lengths) {
                    double accuracy = calcOverallAccuracy(d, len, n, results);
                    writer.write(
                            StringUtils.joinWith(" ", "\t\t\t",
                                    StringUtils.rightPad(d, maxDetectorNameLength, " "), "len=" + len, "noise=" + denoise(n),
                                    "accuracy=" + df.format(accuracy))
                    );
                    writer.newLine();
                }
            }
        }

        writer.newLine();
        writer.write("CONFIDENCE SCORES -- Detector/Noise/Length");
        writer.newLine();
        for (String d : detectors) {
            writer.write("DETECTOR: " + d);
            writer.newLine();
            for (String n : noise) {
                writer.write("\tNOISE: " + denoise(n));
                writer.newLine();
                for (Integer len : lengths) {
                    SummaryStatistics sm = new SummaryStatistics();
                    double median = calcConfidence(d, len, n, results, sm);
                    writer.write(
                            StringUtils.joinWith(" ", "\t\t\t",
                                    StringUtils.rightPad(d, maxDetectorNameLength, " "), "len=" + len, "noise=" + denoise(n),
                                    "confidence_mean=" + df.format(sm.getMean()),
                                    "confidence_stdev=" + df.format(sm.getStandardDeviation()),
                                    "confidence_median=" + df.format(median)
                            ));
                    writer.newLine();
                }
            }
        }
        writer.newLine();
        writer.write("Accuracy Across Languages -- Length/Noise/Detector");
        writer.newLine();
        for (Integer len : lengths) {
            writer.write("LENGTH: " + len);
            writer.newLine();
            for (String n : noise) {
                writer.write("\tNOISE: " + denoise(n));
                writer.newLine();
                for (String d : detectors) {
                    double accuracy = calcOverallAccuracy(d, len, n, results);
                    writer.write(
                            StringUtils.joinWith(" ", "\t\t\t",
                                    StringUtils.rightPad(d, maxDetectorNameLength, " "), "len=" + len, "noise=" + denoise(n),
                                    "accuracy=" + df.format(accuracy))
                    );
                    writer.newLine();
                }
            }
        }

        writer.newLine();
        writer.write("CONFIDENCE SCORES -- Length/Noise/Detector");
        writer.newLine();
        for (Integer len : lengths) {
            writer.write("LENGTH: " + len);
            writer.newLine();
            for (String n : noise) {
                writer.write("\tNOISE: " + denoise(n));
                writer.newLine();
                for (String d : detectors) {
                    SummaryStatistics sm = new SummaryStatistics();
                    double median = calcConfidence(d, len, n, results, sm);
                    writer.write(
                            StringUtils.joinWith(" ", "\t\t\t",
                                    StringUtils.rightPad(d, maxDetectorNameLength, " "), "len=" + len, "noise=" + denoise(n),
                                    "confidence_mean=" + df.format(sm.getMean()),
                                    "confidence_stdev=" + df.format(sm.getStandardDeviation()),
                                    "confidence_median=" + df.format(median)
                            ));
                    writer.newLine();
                }
            }
        }
    }

    private static String denoise(String n) {
        if (n.equals("0")) {
            return "0";
        }
        if (n.length() == 2) {
            return Double.toString(Double.parseDouble(n) / 100);
        } else if (n.length() == 1) {
            return Double.toString(Double.parseDouble(n) / 10);
        }
        throw new RuntimeException("can't denoise " + n);
    }

    private double calcConfidence(String d, int len, String noise, List<Result> results, SummaryStatistics sm) {
        Median median = new Median();
        List<Double> vals = new ArrayList<>();
        for (Result r : results) {
            if (!r.detector.equals(d)) {
                continue;
            }
            if (r.length != len) {
                continue;
            }
            if (!r.noise.equals(noise)) {
                continue;
            }
            sm.addValue(r.confidence);
            vals.add(r.confidence);
        }
        double[] dv = new double[vals.size()];
        for (int i = 0; i < vals.size(); i++) {
            dv[i] = vals.get(i);
        }
        return median.evaluate(dv);
    }

    private double calcAccuracy(String d, int len, String noise, String lang, List<Result> results) {
        //group by...the stupid way
        int denom = 0;
        int numerator = 0;
        for (Result r : results) {

            if (!r.detector.equals(d)) {
                continue;
            }
            if (r.length != len) {
                continue;
            }
            if (!r.noise.equals(noise)) {
                continue;
            }
            if (!r.expectedlang.equals(lang)) {
                continue;
            }
            if (!r.supported) {
                continue;
            }
            if (r.hit) {
                numerator++;
            }
            denom++;
        }
        if (denom == 0) {
            return -1.0;
        }
        return (double) numerator / (double) denom;
    }


    private double calcOverallAccuracy(String d, int len, String noise, List<Result> results) {
        //group by...the stupid way
        int denom = 0;
        int numerator = 0;
        for (Result r : results) {

            if (!r.detector.equals(d)) {
                continue;
            }
            if (r.length != len) {
                continue;
            }
            if (!r.noise.equals(noise)) {
                continue;
            }
            if (!r.supported) {
                continue;
            }
            if (r.hit) {
                numerator++;
            }
            denom++;
        }
        return (double) numerator / (double) denom;
    }

    private void dump(String detectorName, Integer length, List<Long> longs, BufferedWriter writer) throws IOException {
        SummaryStatistics summaryStatistics = new SummaryStatistics();
        for (Long lng : longs) {
            summaryStatistics.addValue(lng);
        }
        writer.write(detectorName + "\t" + length +
                "\t" + (long) summaryStatistics.getSum() + "\t" +
                df.format(summaryStatistics.getMean()) + "\t" +
                df.format(summaryStatistics.getStandardDeviation())
        );
        writer.newLine();
    }

    private Result processSample(File sampleFile, String string, LangDetector detector) throws Exception {
        String fullLang = "";
        String expectedLang = "";
        int length = -1;
        String noise = "";
        String lang1 = "";
        String lang1Conf = "";
        String lang2 = "";
        String lang2Conf = "";
        int id = -1;
        String src = "";
        double confidence = -1.0;
        m.reset(sampleFile.getName());
        if (m.find()) {
            fullLang = m.group(1);
            expectedLang = m.group(2);
            src = m.group(3);
            length = Integer.parseInt(m.group(4));
            noise = m.group(5);
            id = Integer.parseInt(m.group(6));
        } else {
            throw new IllegalArgumentException(sampleFile.getName());
        }
        long start = System.currentTimeMillis();
        List<LangDetectResult> results = detector.detect(string);
        long elapsed = System.currentTimeMillis() - start;
        if (results.size() > 0) {
            LangDetectResult r = results.get(0);
            lang1 = r.getLanguage();
            confidence = r.getConfidence();
            lang1Conf = confidenceFormat.format(confidence);
        }
        if (results.size() > 1) {
            LangDetectResult r = results.get(1);
            lang2 = r.getLanguage();
            lang2Conf = confidenceFormat.format(r.getConfidence());

        }

        Map<Integer, List<Long>> elapsedTimes = processingTimes.get(detector.getClass().getSimpleName());
        if (elapsedTimes == null) {
            elapsedTimes = new HashMap<>();
            processingTimes.put(detector.getClass().getSimpleName(), elapsedTimes);
        }
        List<Long> times = elapsedTimes.get(length);
        if (times == null) {
            times = new ArrayList<>();
            elapsedTimes.put(length, times);
        }
        times.add(elapsed);
        String hit = isHit(expectedLang, lang1);
        writer.write(StringUtils.joinWith("\t",
                detector.getClass().getSimpleName(),
                sampleFile.getName(),
                fullLang,
                expectedLang,
                src,
                hit,
                length,
                denoise(noise),
                id,
                lang1,
                lang1Conf,
                lang2,
                lang2Conf,
                elapsed
        ) + "\n");
        if (hit.equals("hit")) {
            return new Result(detector.getClass().getSimpleName(),
                    length, noise, id, expectedLang, lang1, confidence,
                    detector.getSupportedLangs().contains(expectedLang), true);
        }
        return new Result(detector.getClass().getSimpleName(),
                length, noise, id, expectedLang, lang1, confidence,
                detector.getSupportedLangs().contains(expectedLang),
                false);
    }

    private String isHit(String expected, String lang) {
        if (expected.equals(lang)) {
            return "hit";
        }
        return "miss";
    }

    private String getExpected(File sampleFile) {
        String n = sampleFile.getName();
        int i = n.indexOf("_");
        return n.substring(0, i);
    }

    private class Result {
        String detector;
        int length;
        String noise;
        int id;
        double confidence;
        String expectedlang;
        String detectedlang;
        boolean supported;
        boolean hit;

        public Result(String detector, int length, String noise, int id,
                      String expectedlang, String detectedlang, double confidence,
                      boolean supported, boolean hit) {
            this.detector = detector;
            this.length = length;
            this.noise = noise;
            this.id = id;
            this.expectedlang = expectedlang;
            this.detectedlang = detectedlang;
            this.confidence = confidence;
            this.supported = supported;
            this.hit = hit;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "detector='" + detector + '\'' +
                    ", length=" + length +
                    ", noise='" + noise + '\'' +
                    ", id=" + id +
                    ", confidence=" + confidence +
                    ", expectedlang='" + expectedlang + '\'' +
                    ", detectedlang='" + detectedlang + '\'' +
                    ", supported=" + supported +
                    ", hit=" + hit +
                    '}';
        }
    }
}
