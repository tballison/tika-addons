package org.tallison.bugs.gitlab;

import org.tallison.bugs.ScraperUtils;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.io.IOException;
import java.io.InputStream;
import java.time.format.DateTimeFormatter;

public class GitlabIssuePageScraper extends DefaultHandler {

    private final String host;

    public GitlabIssuePageScraper(String host) {
        this.host = host;
    }

    public static GitlabIssue parse(String host, InputStream is) throws IOException, SAXException {
        org.ccil.cowan.tagsoup.Parser parser =
                new org.ccil.cowan.tagsoup.Parser();
        GitlabIssuePageScraper scraper = new GitlabIssuePageScraper(host);
        parser.setContentHandler(scraper);

        parser.parse(new InputSource(is));
        return scraper.getPage();
    }

    private GitlabIssue issuePage = new GitlabIssue();
    //2020-08-09T17:20:57Z
    private DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssX");
    public GitlabIssue getPage() {
        return issuePage;
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        if ("a".equals(localName)) {
            String clazz = ScraperUtils.getAttr("class", atts);
            if ("gfm".equals(clazz)) {
                String relHref = ScraperUtils.getAttr("href", atts);
                if (relHref != null) {
                    issuePage.attachedUrls.add(host+relHref);
                }
            }
            String rel = ScraperUtils.getAttr("rel", atts);
            if (rel != null && rel.contains("nofollow")) {
                String href = ScraperUtils.getAttr("href", atts);
                if (href != null) {
                    issuePage.externalUrls.add(href);
                }
            }
        }
        if ("time".equals(localName)) {
            String dateTime = ScraperUtils.getAttr("datetime", atts);
            String placement = ScraperUtils.getAttr("data-placement", atts);
            if ("top".equals(placement) && dateTime != null) {
                issuePage.opened = ScraperUtils.getCreated(dateTimeFormatter, dateTime);
            }
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
    }
}
