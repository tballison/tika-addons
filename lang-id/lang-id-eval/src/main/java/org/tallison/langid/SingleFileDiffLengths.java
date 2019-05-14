package org.tallison.langid;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.tallison.langid.opennlp.OpenNLPLangDetector;

public class SingleFileDiffLengths {
    DecimalFormat df = new DecimalFormat("#.0000");
    public static void main(String[] args) throws Exception {
        int[] lengths = new int[]{
                10,20,50,100,200, 500, 1000,10000, 20000, 30000, 40000,
                50000,60000,70000,80000,90000,100000
        };
        int blockSize = 1000;
        Path p = Paths.get(args[0]);
        LangDetector ld = new OpenNLPLangDetector();
        SingleFileDiffLengths singleFileDiffLengths = new SingleFileDiffLengths();
        singleFileDiffLengths.execute(p, lengths, ld, blockSize);
    }

    private void execute(Path p, int[] lengths, LangDetector ld, int blockSize) throws IOException {
        String txt = FileUtils.readFileToString(p.toFile(), StandardCharsets.UTF_8);
        for (int len : lengths) {
            String s = txt.substring(0, len);
            s = blockShuffle(s,blockSize);
            List<LangDetectResult> results = ld.detect(s);
            String lang1="";
            String conf1="";
            String lang2="";
            String conf2="";
            String lang3="";
            String conf3="";
            if (results.size() > 0) {
                lang1 = results.get(0).getLanguage();
                conf1 = df.format(results.get(0).getConfidence());
            }
            if (results.size() > 1) {
                lang2 = results.get(1).getLanguage();
                conf2 = df.format(results.get(1).getConfidence());
            }
            if (results.size() > 2) {
                lang3 = results.get(2).getLanguage();
                conf3 = df.format(results.get(2).getConfidence());
            }
            System.out.println(StringUtils.joinWith("\t",
                    len, lang1, conf1, lang2, conf2, lang3, conf3));
        }
    }

    private String blockShuffle(String s, int blockSize) {
        if (s.length() <= blockSize) {
            return s;
        }
        List<String> blocks = new ArrayList<>();
        for (int i = 0; i < s.length()-blockSize; i += blockSize) {
            int end = Math.min(s.length(), i+blockSize);
            blocks.add(s.substring(i, end));
        }
        Collections.shuffle(blocks);
        StringBuilder sb = new StringBuilder();
        for (String block : blocks) {
            sb.append(block);
        }
        return sb.toString();
    }
}
