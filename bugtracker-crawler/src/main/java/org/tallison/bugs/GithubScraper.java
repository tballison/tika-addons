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
import org.apache.commons.lang3.StringUtils;
import org.apache.http.client.HttpClient;

import java.io.BufferedReader;
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
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GithubScraper {
    /**
     * /home/tallison/data/github/sumatra SUMATRAPDF https://github.com/sumatrapdfreader/sumatrapdf
     * /home/tallison/data/github/mozilla MOZILLA https://github.com/mozilla/pdf.js
     * /home/tallison/data/github/qpdf QPDF https://github.com/qpdf/qpdf
     * /home/tallison/data/github/openpdf OPENPDF https://github.com/LibrePDF/OpenPDF
     * /home/tallison/data/github/ocrmypdf OCRMYPDF https://github.com/jbarlow83/OCRmyPDF
     * /home/tallison/data/github/laraval-snappy LARAVEL_SNAPPY https://github.com/barryvdh/laravel-snappy
     * /Users/allison/data/github/pdfminer PDFMINER https://github.com/pdfminer/pdfminer.six
     */

    /**
     * TODO see SUMATRA-1343 --need to add links to github repo files;
     * can't just block all of github.com
     */
    static Pattern HREF_PATTERN = Pattern.compile("<a ([^>]*)href=\"([^\"]+)([^>]*)\"?>");
    static Pattern FILES_PATTERN = Pattern.compile("\\/files\\/\\d+");
    static Pattern PULL_PATTERN = Pattern.compile("\\/pull\\/\\d+");
    static Pattern DATE_TIME = Pattern.compile("relative-time datetime=\"([-T:\\d]+Z)\"");
    private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssVV");

    private static final String DOCS = "docs";
    private static final String METADATA = "metadata";

    private Set<String> externalExtensions = new HashSet<>();


    public GithubScraper() {
        //TODO: make this configurable
        externalExtensions.add("pdf");
    }

    public static void main(String[] args) throws Exception {

        String fileOrUrl = args[0];
        Path root = Paths.get(args[1]);
        if (Files.isRegularFile(Paths.get(fileOrUrl))) {
            try (BufferedReader reader = Files.newBufferedReader(Paths.get(fileOrUrl), StandardCharsets.UTF_8)) {
                String baseUrl = reader.readLine();
                while (baseUrl != null) {
                    if (! baseUrl.startsWith("#")) {
                        GithubScraper scraper = new GithubScraper();
                        System.out.println("going to get " + baseUrl);
                        scraper.scrape(root, baseUrl);
                    }
                    baseUrl = reader.readLine();

                }
            }
        } else {
            String baseUrl = fileOrUrl;
            GithubScraper scraper = new GithubScraper();
            scraper.scrape(root, baseUrl);
        }

    }

    private void scrape(Path root, String baseUrl) throws ClientException, IOException {
        String projName = FilenameUtils.getName(baseUrl);
        String lcProjName = projName.toLowerCase(Locale.US);
        Path docsRoot = root.resolve(DOCS).resolve(lcProjName);
        Path metadataRoot = root.resolve(METADATA).resolve(lcProjName);

        Files.createDirectories(metadataRoot);
        Files.createDirectories(docsRoot);
        int maxIssue = getMaxIssue(baseUrl);
        if (maxIssue < 0) {
            throw new RuntimeException("Couldn't find max issue "+maxIssue);
        }
        for (int i = maxIssue; i > -1; i--) {
            processIssue(i, baseUrl, lcProjName, docsRoot, metadataRoot);
        }
    }

    private int getMaxIssue(String baseUrl) throws ClientException {
        byte[] htmlBytes = null;
        String url = baseUrl + "/issues?q=is%3Aissue+sort%3Acreated-desc";
        htmlBytes = HttpUtils.get(url);
        String html = new String(htmlBytes, StandardCharsets.UTF_8);
        Matcher m = Pattern.compile("<div id=\"issue_(\\d+)").matcher(html);
        if (m.find()) {
            return Integer.parseInt(m.group(1));
        }
        return -1;
    }

    private void processIssue(int issueId, String baseUrl,
                              String lcProjName,
                              Path docsRoot, Path metadataRoot) throws ClientException {
        HttpClient httpClient = HttpUtils.getClient(baseUrl);
        //https://github.com/tballison/tika-addons/issues/3
        String url = "https://api.github.com/repos/tballison/tika-addons/issues/3";
        //url = "https://github.com/qpdf/qpdf/issues/391";
        url = baseUrl + "/issues/" + issueId;
        Path htmlFile = metadataRoot.resolve(issueId + ".html");

        String html = getIssueHtml(httpClient, lcProjName, issueId, url, htmlFile);
        if (html == null) {
            return;
        }
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
        Matcher filesMatcher = FILES_PATTERN.matcher("");
        Matcher pullMatcher = PULL_PATTERN.matcher("");
        List<Attachment> attachments = new ArrayList<>();
        List<Attachment> externalLinks = new ArrayList<>();
        href.reset(html);
        Set<String> seenAttachments = new HashSet<>();
        Set<String> seenExternalLinks = new HashSet<>();
        while (href.find()) {
            String hrefString = href.group(2);
            pullMatcher.reset(hrefString);
            if (pullMatcher.find()) {
                continue;
            } else if (href.group(2).contains("opensource.guide")) {
                continue;
            } else if (href.group(2).contains("travis-ci.org")) {
                continue;
            } else if (href.group(2).contains("gist.github")) {
                continue;
            } else if (href.group(2).contains("github.com/notifications")) {
                continue;
            }

            if (hrefString.contains("github.com")) {
                if (hrefString.contains("/commit/") || hrefString.contains("/tree/")) {
                    continue;
                }
                String pre = href.group(1).trim();
                String post = href.group(3).replaceAll("[\\s\"]", "").trim();
                //filter out github.com hrefs with stuff before or after the href within the <a/>
                if (! StringUtils.isAllBlank(pre) || ! StringUtils.isAllBlank(post)) {
                    continue;
                }
            }

            filesMatcher.reset(hrefString);

//            System.out.println(hrefString);
            if (filesMatcher.find()) {
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

        getFiles(httpClient, docsRoot, lcProjName, issueId, attachments);
        getExternalLinks(docsRoot, lcProjName, issueId, externalLinks);
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

    private String getIssueHtml(HttpClient httpClient, String project, int issueId, String url, Path htmlFile) {
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
                System.out.println("going to get "+project + ": "+issueId);
                htmlBytes = HttpUtils.get(httpClient, url);
            } catch (Exception e) {
                if (e instanceof ClientException) {
                    System.err.println("problem with "+ url +
                            " : " + ((ClientException)e).getMessage());
                } else {
                    e.printStackTrace();
                }
                return null;
            } finally {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    //
                }
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

    private void getFiles(HttpClient httpClient,
                          Path docsRoot, String projName, int issueId, List<Attachment> attachments) {

        int i = 0;
        for (Attachment attachment : attachments) {
            try {
                ScraperUtils.grabAttachment(httpClient, docsRoot, attachment,
                        projName + "-" + issueId, i);
            } catch (IOException e) {
                e.printStackTrace();
                continue;
            }
            i++;
        }
    }

    private void getExternalLinks(Path docsRoot, String projName,
                                  int issueId, List<Attachment> attachments) {

        int i = 0;
        for (Attachment attachment : attachments) {
            System.out.println("grabbing: " + attachment);
            try {
                ScraperUtils.grabAttachment(
                        HttpUtils.getClient(attachment.attachmentUrl),
                        docsRoot, attachment,
                      projName + "-LINK-" + issueId, i);
            } catch (IOException | ClientException e) {
                e.printStackTrace();
                continue;
            }
            i++;
        }
    }
}
