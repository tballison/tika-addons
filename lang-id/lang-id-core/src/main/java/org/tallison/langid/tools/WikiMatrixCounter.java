package org.tallison.langid.tools;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

/**
 * simple class to count stats in wikimatrix data
 *https://github.com/facebookresearch/LASER/blob/master/tasks/WikiMatrix/list_of_bitexts.txt
 *
 */
public class WikiMatrixCounter {
    public static void main(String[] args) throws Exception {
        Path p = Paths.get(args[0]);
        int rows = 0;
        int tokens = 0;
        try (BufferedReader r =
                new BufferedReader(new InputStreamReader(
                     new GzipCompressorInputStream(Files.newInputStream(p)), StandardCharsets.UTF_8))) {
            String line = r.readLine();
            while (line != null) {
                String[] cols = line.split("\t");
                double score = Double.parseDouble(cols[0]);
                String leftLang = cols[1];
                String rightLang = cols[2];

                rows++;
                tokens += LuceneTokenCounter.count(rightLang);
                line = r.readLine();
            }
        }
        System.out.println(p.getFileName()+ " rows: "+rows+" tokens: "+tokens);
    }
}
