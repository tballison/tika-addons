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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GithubScraper {

    static Pattern HREF_PATTERN = Pattern.compile("href=\"([^\"]+)");
    static Pattern FILES_PATTERN = Pattern.compile("\\/files\\/\\d+");
    static Pattern DATE_TIME = Pattern.compile("relative-time datetime=\"([-T:\\d]+Z)\"");
    private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssVV");

    private static final String DOCS = "docs";
    private static final String METADATA = "metadata";

    private final Path fileRoot;
    private final String projName;
    private final String baseUrl;
    private final int maxIssue;

    public GithubScraper(Path fileRoot, String projName, String baseUrl, int maxIssue) {
        this.fileRoot = fileRoot;
        this.projName = projName;
        this.baseUrl = baseUrl;
        this.maxIssue = maxIssue;
    }

    public static void main(String[] args) throws Exception {

        Path fileRoot = Paths.get(args[0]);
        String projName = args[1];
        String baseUrl = args[2];
        int maxIssue = Integer.parseInt(args[3]);
        GithubScraper scraper = new GithubScraper(fileRoot, projName, baseUrl, maxIssue);
        scraper.scrape();

    }

    private void scrape() throws IOException {
        Files.createDirectories(fileRoot.resolve(METADATA));
        Files.createDirectories(fileRoot.resolve(DOCS));
        for (int i = 1; i < maxIssue; i++) {
            processIssue(i);
        }
    }

    private void processIssue(int issueId) {
        //https://github.com/tballison/tika-addons/issues/3
        String url = "https://api.github.com/repos/tballison/tika-addons/issues/3";
        url = "https://github.com/qpdf/qpdf/issues/391";
        url = baseUrl+"/issues/"+issueId;

        byte[] htmlBytes = null;
        try {
            htmlBytes = HttpUtils.get(url);
        } catch (Exception e) {
            System.err.println("problem "+url);
            e.printStackTrace();
            return;
        }
        try {
            Files.write(fileRoot.resolve("metadata/"+issueId+".html"), htmlBytes);
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        String html = new String(htmlBytes, StandardCharsets.UTF_8);
        Matcher dt = DATE_TIME.matcher(html);
        Instant lastModified = null;
        if (dt.find()) {
            String dtString = dt.group(1);
            lastModified = ScraperUtils.getCreated(formatter, dtString);
        } else {
            System.err.println("couldn't find date: "+html);
            return;
        }
        Matcher href = HREF_PATTERN.matcher(html);
        Matcher filesMatcher = FILES_PATTERN.matcher("");
        List<Attachment> attachments = new ArrayList<>();
        href.reset(html);

        while (href.find()) {
            String hrefString = href.group(1);
            filesMatcher.reset(hrefString);
            System.out.println(hrefString);
            if (filesMatcher.find()) {
                System.out.println("FILE!: " + hrefString);
                String f = FilenameUtils.getName(hrefString);
                Attachment attachment = new Attachment(hrefString, f, lastModified);
                attachments.add(attachment);
            }
        }
        getFiles(issueId, attachments);
    }

    private void getFiles(int issueId, List<Attachment> attachments) {

        int i = 0;
        for (Attachment attachment : attachments) {
            try {
                ScraperUtils.grabAttachment(fileRoot.resolve(DOCS), attachment,
                        projName+"-"+issueId, i);
            } catch (IOException e) {
                e.printStackTrace();
                continue;
            }
            i++;
        }
    }
}
