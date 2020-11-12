/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.tallison.bugs.gitlab;

import org.apache.commons.io.FilenameUtils;
import org.apache.http.client.HttpClient;
import org.tallison.bugs.Attachment;
import org.tallison.bugs.ClientException;
import org.tallison.bugs.HttpUtils;
import org.tallison.bugs.ScraperUtils;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Work in progress. TODO: need to update to
 * read urls from file
 */
public class GitlabScraper {
    /**
     *
     * /home/tallison/data/gitlab https://gitlab.freedesktop.org/poppler/poppler/
     * /home/tallison/data/gitlab https://gitlab.freedesktop.org/cairo/cairo/
     * /home/tallison/data/gitlab https://gitlab.gnome.org/GNOME/evince
     *
     */
    static Pattern HREF_PATTERN = Pattern.compile("<a ([^>]*)href=\"([^\"]+)([^>]*>)");

    //this is really flimsy...
    static Pattern DATE_TIME = Pattern.compile("datetime=\"([-T:\\d]+Z)\"");
    private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssVV");

    private static final String DOCS = "docs";
    private static final String METADATA = "metadata";

    private final Path fileRoot;
    private final String projName;
    private final String baseUrl;
    private final String host;
    private Set<String> externalExtensions = new HashSet<>();


    public GitlabScraper(Path fileRoot, String projName, String baseUrl) throws MalformedURLException {
        this.fileRoot = fileRoot;
        this.projName = projName;
        this.baseUrl = baseUrl;
        URL base = new URL(baseUrl);
        this.host = base.getProtocol()+"://"+base.getHost();
        externalExtensions.add("pdf");
    }

    public static void main(String[] args) throws Exception {

        Path fileRoot = Paths.get(args[0]);
        String baseUrl = args[1];
        baseUrl = baseUrl.replaceAll("/+\\Z", "");
        String projName = FilenameUtils.getName(baseUrl);

        if (! baseUrl.endsWith("/")) {
            baseUrl += "/";
        }
        GitlabScraper scraper = new GitlabScraper(fileRoot, projName, baseUrl);
        scraper.scrape();

    }

    private void scrape() throws Exception {
        Files.createDirectories(fileRoot.resolve(METADATA+"/"+projName));
        Files.createDirectories(fileRoot.resolve(DOCS+"/"+projName));
        int page = 0;
        int maxPage = -1;
        HttpClient client = HttpUtils.getClient(baseUrl);
        while (++page <= maxPage || maxPage < 0) {
            GitlabResultsPage resultsPage = getPage(page, client);
            if (resultsPage.maxPage < 0) {
                System.err.println("bad max page from pageNum" + page);
                break;
            }
            if (maxPage < 0 && resultsPage.maxPage > 0) {
                maxPage = resultsPage.maxPage;
            }
            for (String relativeIssuePageUrl : resultsPage.urls) {
                try {
                    processIssue(relativeIssuePageUrl, client);
                } catch (IOException|SAXException e) {
                    System.err.println("problem with "+relativeIssuePageUrl);
                    e.printStackTrace();
                }
            }
        }
    }

    private GitlabResultsPage getPage(int pageNum, HttpClient httpClient) throws Exception {
//        https://gitlab.freedesktop.org/poppler/poppler/-/issues?page=2&scope=all&state=all
        String url = baseUrl + "issues?page="+pageNum+"&scope=all&state=all";
        byte[] htmlBytes = HttpUtils.get(httpClient, url);
        return GitlabResultsPageScraper.parse(
                new ByteArrayInputStream(htmlBytes)
        );
    }

    private void processIssue(String relativeIssueUrl, HttpClient httpClient) throws IOException, SAXException {
        //https://gitlab.freedesktop.org/poppler/poppler/issues/885
        String issueIdString = FilenameUtils.getName(relativeIssueUrl);
        int issueId = Integer.parseInt(issueIdString);
        String url = host+relativeIssueUrl;
        Path htmlFile = fileRoot.resolve("metadata/" + projName + "/" +issueId+ ".html");

        String html = getIssueHtml(httpClient, issueId, url, htmlFile);
        if (html == null) {
            return;
        }

        GitlabIssue issue = GitlabIssuePageScraper.parse(host, new ByteArrayInputStream(
                html.getBytes(StandardCharsets.UTF_8)));


        Set<String> seenAttachments = new HashSet<>();
        Set<String> seenExternalLinks = new HashSet<>();

        List<Attachment> attachments = new ArrayList<>();
        List<Attachment> externalLinks = new ArrayList<>();

        for (String attachmentHref : issue.attachedUrls) {
            if (seenAttachments.contains(attachmentHref)) {
                continue;
            }
            String f = FilenameUtils.getName(attachmentHref);
            attachments.add(new Attachment(attachmentHref, f, issue.opened));
            seenAttachments.add(attachmentHref);
        }

        for (String hrefString : issue.externalUrls) {
            Attachment attachment = getExternalLink(hrefString, issue.opened, seenExternalLinks);
            if (attachment != null) {
                externalLinks.add(attachment);
            }
        }
        getFiles(httpClient, issueId, attachments);
        getExternalLinks(issueId, externalLinks);
    }

    private Attachment getExternalLink(String hrefString, Instant lastModified,
                                       Set<String> externalLinks) {
        if (hrefString.contains("gitlab") || hrefString.contains("www.adobe.com")) {
            return null;
        }
        URI url = null;
        try {
            url = new URI(hrefString);
        } catch (URISyntaxException e) {
            return null;
        }
        String ext = FilenameUtils.getExtension(url.getPath());
        if (! externalExtensions.contains(ext)) {
            return null;
        }
        String actualURL = url.getScheme()+"://"+
                url.getHost()+url.getRawPath();
        if (externalLinks.contains(actualURL)) {
            return null;
        } else {
            externalLinks.add(actualURL);
        }
        String f = FilenameUtils.getName(url.getPath());

        return new Attachment(actualURL, f, lastModified);
    }

    private String getIssueHtml(HttpClient client, int issueId, String url, Path htmlFile) {
        byte[] htmlBytes = null;
        if (Files.isRegularFile(htmlFile)) {
            System.out.println("processing existing issue: "+issueId);
            try {
                htmlBytes = Files.readAllBytes(htmlFile);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        } else {
            try {
                System.out.println("going to get "+issueId + " : "+url);
                htmlBytes = HttpUtils.get(client, url);
                Thread.sleep(1000);
            } catch (Exception e) {
                if (e instanceof ClientException) {
                    System.err.println("problem with "+ url +
                            " : " + ((ClientException)e).getMessage());
                } else {
                    e.printStackTrace();
                }
                return null;
            }
            try {
                Files.write(htmlFile, htmlBytes);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }
        return new String(htmlBytes, StandardCharsets.UTF_8);

    }

    private void getFiles(HttpClient httpClient, int issueId, List<Attachment> attachments) {

        int i = 0;
        for (Attachment attachment : attachments) {
            try {
                ScraperUtils.grabAttachment(httpClient, fileRoot.resolve(DOCS+"/"+projName), attachment,
                        projName + "-" + issueId, i);
            } catch (IOException e) {
                e.printStackTrace();
                continue;
            }
            i++;
        }
    }

    private void getExternalLinks(int issueId, List<Attachment> attachments) {

        int i = 0;
        for (Attachment attachment : attachments) {
            System.out.println("grabbing: " + attachment);
            try {
                ScraperUtils.grabAttachment(fileRoot.resolve(DOCS+"/"+projName), attachment,
                      projName + "-LINK-" + issueId, i);
            } catch (ClientException|IOException e) {
                e.printStackTrace();
                continue;
            } catch (Exception e) {
                e.printStackTrace();
            }
            i++;
        }
    }
}
