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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import opennlp.tools.langdetect.LanguageDetector;
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

    Matcher m = Pattern.compile("(([a-z]+)(?:-[a-z]+)?)_(\\d+)_0_(\\d+)").matcher("");
    //detector, length, processing time
    Map<String, Map<String, List<Long>>> processingTimes = new HashMap<>();
    DecimalFormat df = new DecimalFormat("#.##");
    DecimalFormat confidenceFormat = new DecimalFormat("#.####");


    private final Writer writer;
    public static void main(String[] args) throws Exception {
        Path sampleDir = Paths.get(args[0]);
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(args[1]), StandardCharsets.UTF_8)) {

            LangDetector[] detectors = new LangDetector[]{
                    new YalderDetector(),
                    new OptimaizeLangDetector(),
                    new OpenNLPLangDetector(),
            };
            LangDetectRunner runner = new LangDetectRunner(writer);
            List<Result> results = new ArrayList<>();
            for (LangDetector detector : detectors) {
                runner.execute(sampleDir, detector, results);
            }
            runner.dumpResults(detectors, results);
        }

    }

    public LangDetectRunner(Writer writer) throws Exception {
        this.writer = writer;
        writer.write(StringUtils.joinWith("\t",
                "detector",
                "sampleFile",
                "fullLang",
                "expectedLang",
                "hit",
                "length",
                "noise",
                "lang1",
                "lang1Conf",
                "lang2",
                "lang2Conf",
                "elapsed(ms)")+"\n");
    }

    private void execute(Path sampleDir, LangDetector detector, List<Result> results) {
        for (File subdir : sampleDir.toFile().listFiles()) {
            for (File sampleFile : subdir.listFiles()) {
                try {
                    results.add(processSample(sampleFile, detector));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void dumpResults(LangDetector[] detectorArr, List<Result> results) {
        Set<String> lengths = new LinkedHashSet<>();
        Set<String> noise = new LinkedHashSet<>();
        Set<String> detectors = new LinkedHashSet<>();
        Set<String> langs = new LinkedHashSet<>();
        for (Result r : results) {
            lengths.add(r.length);
            noise.add(r.noise);
            detectors.add(r.detector);
            langs.add(r.expectedlang);
        }
        System.out.println("Accuracy Across Languages");
        for (String d : detectors) {
            System.out.println("DETECTOR: "+d);
            for (String len : lengths) {
                System.out.println("\tLENGTH: "+len);
                for (String n : noise) {
                    System.out.println("\t\tNOISE: "+denoise(n));
                    double accuracy = calcAccuracy(d, len, n, results);
                    System.out.println(
                            StringUtils.joinWith(" ","\t\t\t",
                                    d, "len="+len, "noise="+denoise(n),
                                    "accuracy="+df.format(accuracy))
                    );
                }
            }
        }

        System.out.println("\n");
        System.out.println("CONFIDENCE SCORES");
        for (String d : detectors) {
            System.out.println("DETECTOR: "+d);
            for (String len : lengths) {
                System.out.println("\tLENGTH: "+len);
                for (String n : noise) {
                    System.out.println("\t\tNOISE: "+denoise(n));
                    SummaryStatistics sm = new SummaryStatistics();
                    double median = calcConfidence(d, len, n, results, sm);
                    System.out.println(
                            StringUtils.joinWith(" ","\t\t\t",
                                    d, "len="+len, "noise="+denoise(n),
                                    "mean="+df.format(sm.getMean()),
                                    "stdev="+df.format(sm.getStandardDeviation()),
                                    "median="+df.format(median)
                    ));
                }
            }
        }

        System.out.println("\nCovered Languages");
        for (LangDetector d : detectorArr) {
            int covered = 0;
            for (String l : langs) {
                if (d.getSupportedLangs().contains(l)) {
                    covered++;
                }
            }
            System.out.println("DETECTOR: "+d.getClass().getSimpleName()+
                    ("("+covered +" out of "+langs.size()+")"));
            for (String l : langs) {
                System.out.println("\t"+l+"\t"+d.getSupportedLangs().contains(l));
            }
        }

        System.out.println("\nTime in Millis");
        System.out.println(
                "Detector\tLength\tMillis\tAvg(ms)\tStdev"
        );
        for (String d : detectors) {
            for (Map.Entry<String, List<Long>> e : processingTimes.get(d).entrySet()) {
                dump(d, e.getKey(), e.getValue());
            }
        }

    }

    private static String denoise(String n) {
        if (n.equals("0")) {
            return "0";
        }
        if (n.length() == 2) {
            return Double.toString(Double.parseDouble(n)/100);
        } else if (n.length() == 1) {
            return Double.toString(Double.parseDouble(n)/10);
        }
        throw new RuntimeException("can't denoise "+n);
    }

    private double calcConfidence(String d, String len, String noise, List<Result> results, SummaryStatistics sm) {
        Median median = new Median();
        List<Double> vals = new ArrayList<>();
        for (Result r : results) {
            if (! r.detector.equals(d)) {
                continue;
            }
            if (! r.length.equals(len)) {
                continue;
            }
            if (! r.noise.equals(noise)) {
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

    private double calcAccuracy(String d, String len, String noise, List<Result> results) {
        //group by...the stupid way
        int denom = 0;
        int numerator = 0;
        for (Result r : results) {

            if (! r.detector.equals(d)) {
                continue;
            }
            if (! r.length.equals(len)) {
                continue;
            }
            if (! r.noise.equals(noise)) {
                continue;
            }
            if (! r.supported) {
                continue;
            }
            if (r.hit) {
                numerator++;
            }
            denom++;
        }
        return (double)numerator/(double)denom;
    }

    private void dump(String detectorName, String key, List<Long> longs) {
        SummaryStatistics summaryStatistics = new SummaryStatistics();
        for (Long lng : longs) {
            summaryStatistics.addValue(lng);
        }
        System.out.println(detectorName + "\t" +key +
                "\t"+ (long)summaryStatistics.getSum() + "\t" +
                df.format(summaryStatistics.getMean()) + "\t"+
                df.format(summaryStatistics.getStandardDeviation())
        );
    }

    private Result processSample(File sampleFile, LangDetector detector) throws Exception {
        String fullLang = "";
        String expectedLang = "";
        String length = "";
        String noise = "";
        String lang1 = "";
        String lang1Conf = "";
        String lang2 = "";
        String lang2Conf = "";
        double confidence = -1.0;
        m.reset(sampleFile.getName());
        if (m.find()) {
            fullLang = m.group(1);
            expectedLang = m.group(2);
            length = m.group(3);
            noise = m.group(4);
        } else {
            throw new IllegalArgumentException(sampleFile.getName());
        }
        long start = System.currentTimeMillis();
        List<LangDetectResult> results = detector.detect(FileUtils.readFileToString(sampleFile, StandardCharsets.UTF_8));
        long elapsed = System.currentTimeMillis()-start;
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

        Map<String, List<Long>> elapsedTimes = processingTimes.get(detector.getClass().getSimpleName());
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
                hit,
                length,
                noise,
                lang1,
                lang1Conf,
                lang2,
                lang2Conf,
                elapsed
        )+"\n");
        if (hit.equals("hit")) {
            return new Result(detector.getClass().getSimpleName(),
                    length, noise, expectedLang, lang1, confidence,
                    detector.getSupportedLangs().contains(expectedLang), true);
        }
        return new Result(detector.getClass().getSimpleName(),
                length, noise, expectedLang, lang1, confidence,
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
        String length;
        String noise;
        double confidence;
        String expectedlang;
        String detectedlang;
        boolean supported;
        boolean hit;

        public Result(String detector, String length, String noise,
                      String expectedlang, String detectedlang, double confidence,
                      boolean supported, boolean hit) {
            this.detector = detector;
            this.length = length;
            this.noise = noise;
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
                    ", length='" + length + '\'' +
                    ", noise='" + noise + '\'' +
                    ", confidence=" + confidence +
                    ", expectedlang='" + expectedlang + '\'' +
                    ", detectedlang='" + detectedlang + '\'' +
                    ", supported=" + supported +
                    ", hit=" + hit +
                    '}';
        }
    }
}
