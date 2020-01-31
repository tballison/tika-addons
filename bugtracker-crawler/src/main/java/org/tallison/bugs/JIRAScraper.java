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
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Basic scraper to grab attachments from JIRA.
 * <p>
 * Thanks to Cassandra Targett for the links on the JIRA rest api!
 * <p>
 * TODO: error handling/logging etc...
 */
public class JIRAScraper {
    //https://issues.apache.org/jira/rest/api/2/search?jql=project=PDFBOX
    // &fields=key,issuetype,status,summary,attachment

    private static String BASE = "https://issues.apache.org/jira/rest/api/2/search?jql=project=";
    private static String FIELDS = "&fields=key,issuetype,status,summary,attachment";
    private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");

    int maxResults = 200;
    private String project;
    private Path outputDir;

    public JIRAScraper(String project, Path outputDir) {
        this.project = project;
        this.outputDir = outputDir;
    }

    public static void main(String[] args) throws Exception {
        Path outputDir = Paths.get(args[1]);
        Files.createDirectories(outputDir);
        JIRAScraper scraper = new JIRAScraper(args[0], outputDir);
        scraper.execute();
    }

    private void execute() throws IOException, ClientException {
        int start = 0;
        int total = -1;
        while (total < 0 || start < total) {
            String url = BASE + project + FIELDS + "&startAt=" + start + "&maxResults=" + maxResults;
            byte[] jsonBytes = HttpUtils.get(url);
            writeJson(start, jsonBytes);
            String json = new String(jsonBytes, StandardCharsets.UTF_8);
            JsonElement el = JsonParser.parseString(json);
            JsonObject root = (JsonObject) el;
            total = root.getAsJsonPrimitive("total").getAsInt();
            JsonArray arr = root.getAsJsonArray("issues");
            for (JsonElement issueElement : arr) {
                JsonObject issueObj = (JsonObject) issueElement;
                String issueId = issueObj.getAsJsonPrimitive("key").getAsString();
                List<Attachment> attachments = extractAttachments(issueObj);
                for (int i = 0; i < attachments.size(); i++) {
                    Attachment a = attachments.get(i);
                    ScraperUtils.grabAttachment(outputDir, a, issueId, i);
                }
                System.out.println("isu " + issueId + " " + total);
            }
            start += arr.size();
            if (arr.size() == 0) {
                break;
            }
        }

        //System.out.println(html);
    }

    private void writeJson(int start, byte[] jsonBytes) throws IOException {
        if (! Files.isDirectory(outputDir.resolve("metadata"))) {
            Files.createDirectories(outputDir.resolve("metadata"));
        }
        Path targ = outputDir.resolve("metadata/"+project+"-"+start+".json");
        Files.write(targ, jsonBytes);
    }




    private List<Attachment> extractAttachments(JsonObject issueObj) {
        JsonObject fields = issueObj.getAsJsonObject("fields");
        JsonArray attachmentArr = fields.getAsJsonArray("attachment");
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



}
