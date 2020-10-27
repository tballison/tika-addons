package org.tallison.bugs.gitlab;

import org.apache.tika.sax.OfflineContentHandler;
import org.ccil.cowan.tagsoup.Schema;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.io.IOException;
import java.io.InputStream;

import static org.tallison.bugs.ScraperUtils.getAttr;

public class GitlabResultsPageScraper extends DefaultHandler {


    public static GitlabResultsPage parse(InputStream is) throws IOException, SAXException {
        org.ccil.cowan.tagsoup.Parser parser =
                new org.ccil.cowan.tagsoup.Parser();
        GitlabResultsPageScraper scraper = new GitlabResultsPageScraper();
        parser.setContentHandler(scraper);

        parser.parse(new InputSource(is));
        return scraper.getPage();
    }

    private GitlabResultsPage page = new GitlabResultsPage();

    public GitlabResultsPage getPage() {
        return page;
    }

    boolean inPageTitle = false;
    boolean inLastElement = false;
    StringBuilder lastPageBuffer = new StringBuilder();
    @Override
    public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
        if ("div".equals(localName)) {
            String clazz = getAttr("class", atts);
            if (clazz != null && clazz.contains("issue-title")) {
                inPageTitle = true;
            }
        }
        if (inPageTitle && "a".equals(localName)) {
            String href = getAttr("href", atts);
            if (href != null) {
                page.urls.add(href);
            }
        }
        if ("li".equals(localName)) {
            String clazz = getAttr("class", atts);
            if (clazz != null && clazz.contains("js-last-button")) {
                inLastElement = true;
            }
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        if ("div".equals(localName) && inPageTitle) {
            inPageTitle = false;
        }
        if (inLastElement && "li".equals(localName)) {
            String lastPageString = lastPageBuffer.toString();
            try {
                page.maxPage = Integer.parseInt(lastPageString.trim());
            } catch (NumberFormatException e) {

            }
            lastPageBuffer.setLength(0);
            inLastElement = false;
        }
    }

    @Override
    public void characters(char[] ch, int start, int length) throws SAXException {
        if (inLastElement) {
            lastPageBuffer.append(ch, start, length);
        }
    }
}
