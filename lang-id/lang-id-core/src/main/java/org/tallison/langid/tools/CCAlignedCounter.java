package org.tallison.langid.tools;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.compress.compressors.xz.XZCompressorInputStream;

public class CCAlignedCounter {
    public static void main(String[] args) throws Exception {
        Path p = Paths.get(args[0]);
        int rows = 0;
        int tokens = 0;
        try (BufferedReader r =
                     new BufferedReader(new InputStreamReader(
                             new XZCompressorInputStream(Files.newInputStream(p)), StandardCharsets.UTF_8))) {
            String line = r.readLine();
            while (line != null) {
                String[] cols = line.split("\t");
                String domain= cols[0];
                String leftUrl = cols[1];
                String leftLang = cols[2];
                String rightUrl = cols[3];
                String rightLang = cols[4];

                rows++;
                tokens += LuceneTokenCounter.count(rightLang);
                line = r.readLine();
            }
        }
        System.out.println(p.getFileName()+ " rows: "+rows+" tokens: "+tokens);
    }
}
