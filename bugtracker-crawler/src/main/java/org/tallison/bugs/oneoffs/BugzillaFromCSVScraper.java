package org.tallison.bugs.oneoffs;

import org.tallison.bugs.Attachment;
import org.tallison.bugs.ClientException;
import org.tallison.bugs.HttpUtils;
import org.tallison.bugs.ScraperUtils;
import org.xml.sax.SAXException;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * This is a one-off scraper that ignores the bugzilla API and does literal
 * html scraping.  We had to implement this because bugs.freedesktop.org has turned
 * off its API (as of this writing), even though humans can still search the content.
 *
 * The input is a csv file of results from the bugzilla site.
 *
 * A query looks like this:
 *
 * https://bugs.freedesktop.org/buglist.cgi?bug_status=UNCONFIRMED&bug_status=NEW&bug_status=ASSIGNED&bug_status=REOPENED&bug_status=RESOLVED&bug_status=VERIFIED&bug_status=CLOSED&bug_status=NEEDINFO&bug_status=PLEASETEST&f1=attach_data.thedata&limit=0&o1=isnotempty&order=changeddate%2Cproduct%2Cbug_status%2Cpriority%2Cassigned_to%2Cbug_id&product=DejaVu&query_format=advanced&resolution=---&resolution=FIXED&resolution=INVALID&resolution=WONTFIX&resolution=DUPLICATE&resolution=WORKSFORME&resolution=MOVED&resolution=NOTABUG&resolution=NOTOURBUG
 *
 * To get your content, change the "&project=" to the project you want, run the query manually
 * and then select "csv" at the bottom of the page.
 *
 * then the commandline is, e.g. poppler bugs-2020-10-23-poppler.csv .
 *
 * We should use an actual CSV parser.
 *
 * Projects relevant to tika include:
 *
 * LibreOffice
 * DejaVu
 * colord (icc files)
 * poppler
 * cairo
 * cairo-java
 * cairomm
 *
 * LibreOffice has been entirely migrated to bugs.documentfoundation.org, e.g.: https://bugs.documentfoundation.org/show_bug.cgi?id=45372
 *
 * Some of poppler (since 2018???) have been moved over to gitlab: https://gitlab.freedesktop.org/poppler/poppler
 * But it looks like there are a bunch of issues that have not been migrated over.
 *
 * Not sure where the current issue trackers are for colord, cairo(s) and DejaVu (github?)
 *
 */
public class BugzillaFromCSVScraper {

    //url for requesting specific bug by id
    private final String hostBase = "https://bugs.freedesktop.org/";
    private final String bugURL = hostBase+ "show_bug.cgi?id=";

    public static void main(String[] args) throws Exception {
        String project = args[0];
        Path csv = Paths.get(args[1]);
        Path rootDir = Paths.get(args[2]);

        BugzillaFromCSVScraper scraper = new BugzillaFromCSVScraper();
        scraper.execute(project, csv, rootDir);
    }

    private void execute(String project, Path csv, Path rootDir) throws IOException {
        Path metadataDir = rootDir.resolve("metadata/"+project);
        Path docsDir = rootDir.resolve("docs/"+project);
        Files.createDirectories(metadataDir);
        Files.createDirectories(docsDir);

        //use a real csv parser at some point
        try (BufferedReader reader = Files.newBufferedReader(csv, StandardCharsets.UTF_8)) {
            String line = reader.readLine();
            while (line != null) {
                String[] data = line.split(",");
                String idString = data[0];
                try {
                    processIssue(project, Integer.parseInt(idString), metadataDir, docsDir);
                } catch (Exception e) {
                    //skip
                }
                line = reader.readLine();
            }
        }
    }

    private void processIssue(String project, int issueId, Path metadataDir, Path docsDir) throws ClientException, IOException, SAXException {
        Path issueHtml = metadataDir.resolve(Integer.toString(issueId)+".html");
        byte[] htmlBytes = null;
        if (Files.isRegularFile(issueHtml)) {
            htmlBytes = Files.readAllBytes(issueHtml);
        } else {
            String url = bugURL+issueId;
            htmlBytes = HttpUtils.get(url);
            Files.write(issueHtml, htmlBytes);
        }
        BugzillaIssuePage page = BugzillaHtmlIssueScraper.parse(new ByteArrayInputStream(htmlBytes));

        int i = 0;
        for (String attachmentUrl : page.attachmentUrls) {
            String extension = page.extensions.get(i);
            Attachment attachment = new Attachment(hostBase+attachmentUrl, "something"+extension, page.reportedDate);
            ScraperUtils.grabAttachment(docsDir, attachment,
                    project+"-"+Integer.toString(issueId), i++);
        }
    }
}
