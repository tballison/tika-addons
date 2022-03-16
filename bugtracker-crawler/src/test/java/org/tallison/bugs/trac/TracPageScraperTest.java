package org.tallison.bugs.trac;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.Test;

public class TracPageScraperTest {
    @Test
    public void testPageParsing() throws Exception {
        TracIssuePage page = null;
        //try (InputStream is = getClass().getResourceAsStream("/trac-issue.html")) {
            try (InputStream is =
                         Files.newInputStream(Paths.get("/Users/allison/Desktop/tmp.html"))) {
                 page = TracIssueScraper.parse(is, false);
            }

        System.out.println(page);
    }
}
