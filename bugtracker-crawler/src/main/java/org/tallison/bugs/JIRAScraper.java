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

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.tika.io.FilenameUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Basic scraper to grab attachments from Apache's JIRA.
 * <p>
 * Thanks to Cassandra Targett for the links on the JIRA rest api!
 * <p>
 * TODO: error handling/logging etc...
 */
public class JIRAScraper {

    Matcher urlMatcher = Pattern.compile("\\A(.*)\\/projects\\/(.*)\\Z").matcher("");
    //https://issues.apache.org/jira/rest/api/2/search?jql=project=PDFBOX&fields=key,issuetype,status,summary,attachment

    private static String URL_BASE = "https://issues.apache.org/jira";//https://ec.europa.eu/cefdigital/tracker";//https://issues.apache.org/jira";
    private static String REST_QUERY_BASE = "/rest/api/2/search?jql=project=";
    private static String FIELDS = "&fields=key,issuetype,status,summary,attachment";
    private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

    private static final long SLEEP_MILLIS_BETWEEN_REQUESTS = 1000;


    int maxResults = 200;

    public static void main(String[] args) throws Exception {
        Path jiras = Paths.get(args[0]);
        Path outputRoot = Paths.get(args[1]);
        int maxToFetch = -1;
        if (args.length > 2) {
            maxToFetch = Integer.parseInt(args[2]);
        }
        JIRAScraper scraper = new JIRAScraper();
        if (Files.isRegularFile(jiras)) {
            try (BufferedReader reader = Files.newBufferedReader(jiras, StandardCharsets.UTF_8)) {
                String line = reader.readLine();
                while (line != null) {
                    if (! line.startsWith("#")) {
                        scraper.execute(line.trim(), outputRoot, maxToFetch);
                    }
                    line = reader.readLine();
                }
            }
        } else {
            scraper.execute(args[0], outputRoot, maxToFetch);
        }
    }

    private void execute(String projectUrl, Path outputRoot, int maxToFetch) throws IOException, ClientException {
        urlMatcher.reset(projectUrl);
        String baseUrl = "";
        String project = "";
        if (urlMatcher.find()) {
            baseUrl = urlMatcher.group(1);
            project = urlMatcher.group(2);
        } else {
            System.err.println("Couldn't find \"/projects\" in url: "+projectUrl);
            return;
        }
        Path outputDir = outputRoot.resolve("docs/"+project);
        if (! Files.isDirectory(outputDir)) {
            Files.createDirectories(outputDir);
        }
        int start = 0;
        int total = -1;
        int issueCount = 0;
        if (maxToFetch > 0 && maxToFetch < maxResults) {
            maxResults = maxToFetch;
        }
        while (total < 0 || start < total) {
            String url = baseUrl + REST_QUERY_BASE + project +
                    "%20ORDER%20BY%20updated%20DESC"
                    //+"%20AND%20KEY=PDFBOX-1780"
                    + FIELDS + "&startAt=" + start + "&maxResults=" + maxResults;
            System.err.println("going to get issues: "+url);
            byte[] jsonBytes = HttpUtils.get(url);
            writeJson(outputRoot, project, start, jsonBytes);
            sleepMS(SLEEP_MILLIS_BETWEEN_REQUESTS);
            String json = new String(jsonBytes, StandardCharsets.UTF_8);
            JsonElement el = JsonParser.parseString(json);
            JsonObject root = (JsonObject) el;
            total = root.getAsJsonPrimitive("total").getAsInt();
            JsonArray arr = root.getAsJsonArray("issues");
            System.err.println("got issue numbers "+arr.size());
            for (JsonElement issueElement : arr) {
                JsonObject issueObj = (JsonObject) issueElement;
                String issueId = issueObj.getAsJsonPrimitive("key").getAsString();
                List<Attachment> attachments = extractAttachments(issueObj);
                for (int i = 0; i < attachments.size(); i++) {
                    Attachment a = attachments.get(i);
                    ScraperUtils.grabAttachment(outputDir, a, issueId, i);
                    sleepMS(SLEEP_MILLIS_BETWEEN_REQUESTS);
                }
                issueCount++;
                System.out.println("issue: " + issueId + " " +
                        "start="+start+" issueCount="+issueCount+" total="+ total);
                if (maxToFetch > 0 && issueCount >= maxToFetch) {
                    System.out.println("Fetched up to maxToFetch "+issueCount);
                    return;
                }
            }
            start += arr.size();
            if (arr.size() == 0) {
                break;
            }
        }

        //System.out.println(html);
    }

    private void writeJson(Path outputRoot, String project, int start, byte[] jsonBytes) throws IOException {
        if (! Files.isDirectory(outputRoot.resolve("metadata/"+project))) {
            Files.createDirectories(outputRoot.resolve("metadata/"+project));
        }
        Path targ = outputRoot.resolve("metadata/"+project+"/"+project+"-"+start+".json");
        Files.write(targ, jsonBytes);
    }




    private List<Attachment> extractAttachments(JsonObject issueObj) {
        if (!issueObj.has("fields")) {
            System.err.println("couldn't find field elements: "+issueObj);
            return Collections.EMPTY_LIST;
        }
        JsonObject fields = issueObj.getAsJsonObject("fields");
        JsonArray attachmentArr = fields.getAsJsonArray("attachment");
        if (attachmentArr == null) {
            System.err.println("empty attachment array: "+issueObj);
            return Collections.EMPTY_LIST;
        }
        List<Attachment> attachments = new ArrayList<>();
        for (JsonElement attachment : attachmentArr) {
            String url = ((JsonObject) attachment).getAsJsonPrimitive("content").getAsString();
            String fileName = ((JsonObject) attachment).getAsJsonPrimitive("filename").getAsString();
            System.out.println("attach: " + fileName + " ; " + url);
            Instant created = ScraperUtils.getCreated(formatter,
                    ((JsonObject) attachment)
                    .getAsJsonPrimitive("created").getAsString());
            attachments.add(new Attachment(url, fileName, created));
        }
        return attachments;
    }

    private void sleepMS(long sleepMillis) {
        try {
            System.err.println("about to sleep "+sleepMillis + " ms.");
            Thread.sleep(sleepMillis);
        } catch (InterruptedException e) {
            //swallow
        }
    }

}
