package org.tallison.bugs.oneoffs;

import org.tallison.bugs.ScraperUtils;
import org.tallison.bugs.gitlab.GitlabResultsPage;
import org.tallison.bugs.gitlab.GitlabResultsPageScraper;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.io.IOException;
import java.io.InputStream;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.tallison.bugs.ScraperUtils.getAttr;
import static org.tallison.bugs.ScraperUtils.getCreated;

public class BugzillaHtmlIssueScraper extends DefaultHandler {

    boolean skipPatches = true;

    public static BugzillaIssuePage parse(InputStream is) throws IOException, SAXException {
        org.ccil.cowan.tagsoup.Parser parser =
                new org.ccil.cowan.tagsoup.Parser();
        BugzillaHtmlIssueScraper scraper = new BugzillaHtmlIssueScraper();
        parser.setContentHandler(scraper);

        parser.parse(new InputSource(is));
        return scraper.getPage();
    }

    BugzillaIssuePage page = new BugzillaIssuePage();

    BugzillaIssuePage getPage() {
        return page;
    }

    StringBuilder rowBuffer = new StringBuilder();
    boolean inTableRow = false;
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm z");
    String rawContentType = null;
    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        if ("a".equals(localName)) {
            String title = ScraperUtils.getAttr("title", atts);
            if (title != null && title.startsWith("View the content of the attachment")) {
                String href = ScraperUtils.getAttr("href", atts);
                String extension = getExtension();

                if (skipPatches && ! ".diff".equals(extension)) {
                    page.attachmentUrls.add(href);
                    page.extensions.add(extension);
                }
            }
        }
        if ("tr".equals(localName)) {
            inTableRow = true;
            rawContentType = ScraperUtils.getAttr("class", atts);
        }
    }

    private String getExtension() {
        if (rawContentType == null) {
            return "";
        }
        if (rawContentType.contains("bz_patch")) {
            return ".diff";
        }
        Matcher m = Pattern.compile("bz_contenttype_([^ ]+)").matcher(rawContentType);
        if (m.find()) {
            String mimeString = m.group(1).replaceAll("_", "/");
            String ext = ScraperUtils.getExtensionFromMime(mimeString);
            if (ext != null) {
                return ext;
            }
        }
        return "";
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        if ("tr".equals(localName)) {
            if (page.reportedDate == null) {
                lookForReportedDate();
            }
            inTableRow = false;
            rowBuffer.setLength(0);
            rawContentType = null;
        }
    }

    private void lookForReportedDate() {
        //2009-06-17 08:23 UTC
        Matcher m = Pattern.compile("Reported:\\s+(\\d{4,4}-\\d\\d-\\d\\d \\d\\d:\\d\\d UTC)")
                .matcher(rowBuffer.toString());
        if (m.find()) {
            page.reportedDate = getCreated(formatter, m.group(1));
        }
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        if (inTableRow) {
            rowBuffer.append(ch, start, length);
        }
    }
}
