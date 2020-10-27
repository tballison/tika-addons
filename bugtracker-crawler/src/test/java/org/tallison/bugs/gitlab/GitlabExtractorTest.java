package org.tallison.bugs.gitlab;

import org.apache.http.client.HttpClient;
import org.junit.Test;
import org.tallison.bugs.HttpUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;

public class GitlabExtractorTest {

    @Test
    public void testResultsPageParsing() throws Exception {
        GitlabResultsPage page = null;
        try (InputStream is = getClass().getResourceAsStream("/gitlab_results_page.html")) {
            page = GitlabResultsPageScraper.parse(is);
        }
        System.out.println(page);
    }

    @Test
    public void testIssuePageParsing() throws Exception {
        GitlabIssue page = null;
        try (InputStream is = getClass().getResourceAsStream("/issuePage950.html")) {
            page = GitlabIssuePageScraper.parse("https://gitlab.freedesktop.org/", is);
        }
        System.out.println(page);
    }

    @Test
    public void getOne() throws Exception {
        String url = "https://gitlab.freedesktop.org/poppler/poppler/uploads/ac83e0467117168a7981cc8cd50b7953/poppler-annotation.h";
        HttpClient client = HttpUtils.getClient(url);
        HttpUtils.wget(url, Paths.get("/home/tallison/Desktop/tmp.h"));
        //System.out.println(new String(bytes));
    }

}
