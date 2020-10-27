package org.tallison.bugs.oneoffs;

import org.junit.Test;

import java.io.InputStream;

public class BugzillaHtmlIssueScraperTest {

    @Test
    public void testBasic() throws Exception {
        BugzillaIssuePage page = null;
        try (InputStream is = getClass().getResourceAsStream("/freedesktop-bugzilla-22334.html")) {
            page = BugzillaHtmlIssueScraper.parse(is);
        }
        System.out.println(page);
    }
}
