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
package org.tallison.bugs;

import org.apache.commons.io.FilenameUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
 * Work in progress.  Ignore for now.
 */
public class GitlabScraper {
    /**
     * /home/tallison/data/github/poppler POPPLER https://gitlab.freedesktop.org/poppler/poppler/
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

    private Set<String> externalExtensions = new HashSet<>();


    public GitlabScraper(Path fileRoot, String projName, String baseUrl) {
        this.fileRoot = fileRoot;
        this.projName = projName;
        this.baseUrl = baseUrl;
        //this.domain = // get domain
        externalExtensions.add("pdf");
    }

    public static void main(String[] args) throws Exception {

        Path fileRoot = Paths.get(args[0]);
        String projName = args[1];
        String baseUrl = args[2];
        GitlabScraper scraper = new GitlabScraper(fileRoot, projName, baseUrl);
        scraper.scrape();

    }

    private void scrape() throws ClientException, IOException {
        Files.createDirectories(fileRoot.resolve(METADATA));
        Files.createDirectories(fileRoot.resolve(DOCS));
        int maxIssue = getMaxIssue();
        if (maxIssue < 0) {
            throw new RuntimeException("Couldn't find max issue "+maxIssue);
        }
        for (int i = maxIssue; i > 0; i--) {
            processIssue(i);
        }
    }

    private int getMaxIssue() throws ClientException {
        byte[] htmlBytes = null;
        String url = baseUrl + "/issues";
        htmlBytes = HttpUtils.get(url);
        String html = new String(htmlBytes, StandardCharsets.UTF_8);
        Matcher issueMatcher = Pattern.compile("<li class=\"issue\"([^>]+)>").matcher(html);
        if (issueMatcher.find()) {
            Matcher numMatcher = Pattern.compile("url=\"[^\"]+(\\d+)\"").matcher(issueMatcher.group(1));
            if (numMatcher.find()) {
                return Integer.parseInt(numMatcher.group(1));
            }
        }
        return -1;
    }

    private void processIssue(int issueId) {
        //https://gitlab.freedesktop.org/poppler/poppler/issues/885
        String url = "https://api.github.com/repos/tballison/tika-addons/issues/3";
        url = "https://github.com/qpdf/qpdf/issues/391";
        url = baseUrl + "/issues/" + issueId;
        Path htmlFile = fileRoot.resolve("metadata/" + issueId + ".html");

        String html = getIssueHtml(issueId, url, htmlFile);
        if (html == null) {
            return;
        }
        //get the first.  TODO: fix this to parse xhtml structure
        Matcher dt = DATE_TIME.matcher(html);
        Instant lastModified = null;
        if (dt.find()) {
            String dtString = dt.group(1);
            lastModified = ScraperUtils.getCreated(formatter, dtString);
        } else {
            System.err.println("couldn't find date: " + html);
            return;
        }
        Matcher href = HREF_PATTERN.matcher(html);
        List<Attachment> attachments = new ArrayList<>();
        List<Attachment> externalLinks = new ArrayList<>();
        href.reset(html);
        Set<String> seenAttachments = new HashSet<>();
        Set<String> seenExternalLinks = new HashSet<>();
        while (href.find()) {
            String hrefString = href.group(2);
            if (href.group(2).contains("opensource.guide")) {
                continue;
            } else if (href.group(2).contains("travis-ci.org")) {
                continue;
            }


//            System.out.println(hrefString);
            if (href.group(1) != null && href.group(1).contains("class=\"gfm") ||
                (href.group(3) != null && href.group(3).contains("class=\"gfm"))) {
                if (hrefString.startsWith("/")) {
                    hrefString = baseUrl+hrefString;
                }
                if (seenAttachments.contains(hrefString)) {
                    continue;
                } else {
                    seenAttachments.add(hrefString);
                }
                // System.out.println("FILE!: " + hrefString);
                String f = FilenameUtils.getName(hrefString);
                Attachment attachment = new Attachment(hrefString, f, lastModified);
                attachments.add(attachment);
            } else if (hrefString.startsWith("http:") || hrefString.startsWith("https:")) {
                Attachment attachment = getExternalLink(hrefString, lastModified, seenExternalLinks);
                if (attachment != null) {
                    externalLinks.add(attachment);
                }
            }
        }

        getFiles(issueId, attachments);
        getExternalLinks(issueId, externalLinks);
    }

    private Attachment getExternalLink(String hrefString, Instant lastModified,
                                       Set<String> externalLinks) {
        if (hrefString.contains("github") || hrefString.contains("www.adobe.com")) {
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

    private String getIssueHtml(int issueId, String url, Path htmlFile) {
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
                System.out.println("going to get "+issueId);
                htmlBytes = HttpUtils.get(url);
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

    private void getFiles(int issueId, List<Attachment> attachments) {

        int i = 0;
        for (Attachment attachment : attachments) {
            try {
                ScraperUtils.grabAttachment(fileRoot.resolve(DOCS), attachment,
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
                ScraperUtils.grabAttachment(fileRoot.resolve(DOCS), attachment,
                      projName + "-LINK-" + issueId, i);
            } catch (IOException e) {
                e.printStackTrace();
                continue;
            }
            i++;
        }
    }
}
