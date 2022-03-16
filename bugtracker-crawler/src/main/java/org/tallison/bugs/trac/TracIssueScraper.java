package org.tallison.bugs.trac;

import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.ccil.cowan.tagsoup.HTMLSchema;
import org.ccil.cowan.tagsoup.Schema;
import org.tallison.bugs.ScraperUtils;
import org.tallison.bugs.oneoffs.BugzillaHtmlIssueScraper;
import org.tallison.bugs.oneoffs.BugzillaIssuePage;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import org.apache.tika.utils.XMLReaderUtils;

public class TracIssueScraper extends DefaultHandler {

    public static TracIssuePage parse(InputStream is, boolean pageCrawled) throws IOException,
            SAXException {

        TracIssueScraper scraper = new TracIssueScraper(pageCrawled);

        Schema schema = new HTMLSchema();

        org.ccil.cowan.tagsoup.Parser parser = new org.ccil.cowan.tagsoup.Parser();
        parser.setProperty(org.ccil.cowan.tagsoup.Parser.schemaProperty, schema);
        parser.setContentHandler(scraper);
        parser.parse(new InputSource(is));
        return scraper.getPage();
    }

    private TracIssuePage getPage() {
        return tracIssuePage;
    }

    private final TracIssuePage tracIssuePage;

    public TracIssueScraper(boolean alreadyCrawled) {
        tracIssuePage = new TracIssuePage(alreadyCrawled);
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        if ("link".equals(localName)) {
            String rel = XMLReaderUtils.getAttrValue("rel", atts);
            if (rel != null) {
                if ("next".equals(rel)) {
                    String nextLink = XMLReaderUtils.getAttrValue("href", atts);
                    tracIssuePage.setNextLink(nextLink);
                }
            }
        } else if ("a".equals(localName)) {
            String clss = XMLReaderUtils.getAttrValue("class", atts);
            if ("ext-link".equals(clss)) {
                tracIssuePage.addExternalLink(XMLReaderUtils.getAttrValue("href", atts));
                return;
            } else if ("trac-id".equals(clss)) {
                String idLink = XMLReaderUtils.getAttrValue("href", atts);
                if (idLink == null) {
                    System.err.println("couldn't find idlink");
                    return;
                }
                Matcher m = Pattern.compile("/ticket/(\\d+)").matcher(idLink);
                if (m.find()) {
                    try {
                        tracIssuePage.setIssueId(Integer.parseInt(m.group(1)));
                    } catch (NumberFormatException e) {
                        e.printStackTrace();
                    }
                } else {
                    System.err.println("bad ticket pattern");
                }
                return;
            }

            String href = XMLReaderUtils.getAttrValue("href", atts);
            if (href != null) {
                if (href.startsWith("/zip-attachment")) {
                    tracIssuePage.setZipLink(href);
                }
            }
        }
    }

}
