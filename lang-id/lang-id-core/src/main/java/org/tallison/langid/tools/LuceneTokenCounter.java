package org.tallison.langid.tools;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

public class LuceneTokenCounter {
    private static final Analyzer ANALYZER = new StandardAnalyzer();

    /**
     * Uses lucene's StandardAnalyzer to count tokens
     * @param s
     * @return
     */
    public static int count(String s) throws IOException {
        int cnt = 0;
        try (TokenStream ts = ANALYZER.tokenStream("f", s)) {
            ts.reset();
//            CharTermAttribute ct = ts.getAttribute(CharTermAttribute.class);
            while (ts.incrementToken()) {
                cnt++;
            }
            ts.end();
        }
        return cnt;
    }
}
