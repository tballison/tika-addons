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
import com.google.gson.JsonPrimitive;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BugzillaScraper {

/*
Thanks to @triagegirl for noting that bugzilla has an API!!!
 */

  /*  //all statuses, resolutions.  only application/audio/video ignore text,html
  This initial query didn't actually work programmatically. It worked in the browser manually.
  So, I downloaded it and put it in the right place in the directory.
    private static String INITIAL_QUERY =
            "https://bugs.ghostscript.com/buglist.cgi?bug_status=UNCONFIRMED&bug_status=" +
                    "CONFIRMED&bug_status=IN_PROGRESS&bug_status=AWAITING_REVIEW" +
                    "&bug_status=RESOLVED&bug_status=NOTIFIED&f1=attachments.mimetype" +
                    "&limit=0&o1=anywordssubstr&order=priority%2Cbug_severity" +
                    "&query_format=advanced&resolution=---&resolution=FIXED" +
                    "&resolution=INVALID&resolution=WONTFIX&resolution=LATER" +
                    "&resolution=REMIND&resolution=DUPLICATE&resolution=WORKSFORME" +
                    "&resolution=MOVED&v1=application%20image%20video%20audio";
    private static String ISSUE_URL_BASE = "https://bugs.ghostscript.com/rest.cgi/bug/";
*/

    /*
    //poi
    private static String INITIAL_QUERY =
            "https://bz.apache.org/bugzilla/buglist.cgi?bug_status=UNCONFIRMED&bug_status=NEW" +
                    "&bug_status=ASSIGNED&bug_status=REOPENED&bug_status=NEEDINFO&bug_status=RESOLVED" +
                    "&bug_status=VERIFIED&bug_status=CLOSED&f1=attachments.mimetype&j_top=OR" +
                    "&limit=0&list_id=186475&o1=anywordssubstr&order=priority%2Cbug_severity" +
                    "&product=POI&query_format=advanced&resolution=---&resolution=FIXED" +
                    "&resolution=INVALID&resolution=WONTFIX&resolution=LATER&resolution=REMIND" +
                    "&resolution=DUPLICATE&resolution=WORKSFORME&resolution=MOVED&resolution=CLOSED" +
                    "&resolution=INFORMATIONPROVIDED&v1=application%20image%20video%20audio";

//    private static String ISSUE_URL_BASE = "https://bz.apache.org/bugzilla/rest.cgi/bug/";
//open office
    private static String INITIAL_QUERY = "https://bz.apache.org/ooo/buglist.cgi?bug_status=UNCONFIRMED&bug_status=CONFIRMED&bug_status=ACCEPTED" +
            "&bug_status=REOPENED&bug_status=RESOLVED&bug_status=VERIFIED&bug_status=CLOSED&f1=attachments.mimetype&limit=0&o1=anywordssubstr&order=bug_status%2Cpriority%2Cassigned_to%2Cbug_id" +
            "&query_format=advanced&resolution=---&resolution=FIXED&resolution=FIXED_WITHOUT_CODE" +
            "&resolution=DUPLICATE&resolution=IRREPRODUCIBLE&resolution=NOT_AN_OOO_ISSUE" +
            "&resolution=WONT_FIX&resolution=OBSOLETE&v1=pdf";

    private static String ISSUE_URL_BASE = "https://bz.apache.org/ooo/rest.cgi/bug/";
*/

    //libre office
    private static String INITIAL_QUERY = "https://bugs.documentfoundation.org/buglist.cgi?bug_status=UNCONFIRMED&bug_status=NEW&bug_status=ASSIGNED&bug_status=REOPENED&bug_status=RESOLVED&bug_status=VERIFIED&bug_status=CLOSED&bug_status=NEEDINFO&f1=attachments.mimetype&limit=0&o1=anywordssubstr&order=changeddate%2Cpriority%2Cbug_severity&query_format=advanced&resolution=---&resolution=FIXED&resolution=INVALID&resolution=WONTFIX&resolution=DUPLICATE&resolution=WORKSFORME&resolution=MOVED&resolution=NOTABUG&resolution=NOTOURBUG&resolution=INSUFFICIENTDATA&v1=pdf";
    //note no .cgi!
    private static String ISSUE_URL_BASE = "https://bugs.documentfoundation.org/rest/bug/";
    private DateTimeFormatter formatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssVV");

    private final String project;
    private final Path rootDir;
    private final String apiKey;

    public BugzillaScraper(String project, Path path, String apiKey) {
        this.project = project;
        rootDir = path;
        if (StringUtils.isBlank(apiKey)) {
            this.apiKey = "";
        } else {
            this.apiKey = "?api_key=" + apiKey;
        }
    }

    public static void main(String[] args) throws Exception {
        String project = args[0];
        String apiKey = "";
        if (args.length > 2) {
            apiKey = args[2];
        }
        BugzillaScraper scraper = new BugzillaScraper(project,
                Paths.get(args[1]), apiKey);
        scraper.execute();
    }

    private void execute() throws IOException, ClientException {
        Files.createDirectories(rootDir);
        Files.createDirectories(rootDir.resolve("metadata"));

        Set<String> issueIds = getIssueIds(INITIAL_QUERY);
        List<String> issueList = new ArrayList<>();
        issueList.addAll(issueIds);
        Collections.sort(issueList, Collections.reverseOrder());
        System.out.println("found " + issueList);
        System.out.println("\n\nnum issues: " + issueList.size());
        boolean networkCall = false;
        for (String issueId : issueList) {
            networkCall = processIssue(issueId);
            if (networkCall) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private boolean processIssue(String issueId) throws IOException, ClientException {
        Path jsonMetadataPath = rootDir.resolve("metadata/"+project+"-" + issueId + ".json");
        boolean networkCall = false;
        byte[] jsonBytes = null;
        if (Files.isRegularFile(jsonMetadataPath)) {
            jsonBytes = Files.readAllBytes(jsonMetadataPath);
        } else {
            String url = ISSUE_URL_BASE + issueId + "/attachment" + apiKey;
            networkCall = true;
            try {
                jsonBytes = HttpUtils.get(url);
            } catch (ClientException e) {
                System.err.println(issueId);
                e.printStackTrace();
                return networkCall;
            }
            Files.write(jsonMetadataPath, jsonBytes);
        }
        JsonElement rootEl = JsonParser.parseString(new String(jsonBytes, StandardCharsets.UTF_8));
        if (!rootEl.isJsonObject()) {
            System.err.println("not an obj " + issueId);
            return networkCall;
        }
        JsonElement bugs = rootEl.getAsJsonObject().get("bugs");
        if (bugs.isJsonObject()) {
            int i = 0;
            //should only be one issue=issueid, but why not iterate?
            for (String k : bugs.getAsJsonObject().keySet()) {
                JsonArray arr = bugs.getAsJsonObject().getAsJsonArray(k);
                for (JsonElement el : arr) {
                    if (!el.isJsonObject()) {
                        continue;
                    }
                    processAttachment(issueId, i, el.getAsJsonObject());
                    i++;
                }
            }
        } else if (bugs.isJsonArray()) {
            int i = 0;
            for (JsonElement el : bugs.getAsJsonArray()) {
                if (!el.isJsonObject()) {
                    continue;
                }
                processAttachment(issueId, i, el.getAsJsonObject());
                i++;
            }
        }
        return networkCall;
    }

    private void processAttachment(String issueId, int i, JsonObject attachmentObj) {
        //String contentType = attachmentObj.getAsJsonPrimitive("content_type").getAsString();
        String fileName = attachmentObj.getAsJsonPrimitive("file_name").getAsString();
        String dateString = getAsString(attachmentObj, "last_change_time");
        Instant lastModified = ScraperUtils.getCreated(formatter, dateString);

        Attachment attachment = new Attachment("", fileName, lastModified);

        Path target = ScraperUtils.getInitialTarget(rootDir, attachment,
                project+"-"+issueId, i);
        if (Files.isRegularFile(target)) {
            return;
        }
        String data = getAsString(attachmentObj, "data");
        if (! StringUtils.isAllBlank(data)) {
            byte[] bytes = Base64.getDecoder().decode(data);
            try {
                ScraperUtils.writeAttachment(bytes, target, rootDir, project + "-" + issueId, i, lastModified);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            System.err.println("emtpy data for: "+issueId + " "+i);
        }
    }

    private String getAsString(JsonObject jsonObject, String key) {
        if (jsonObject == null || ! jsonObject.has(key) ) {
            return "";
        }
        JsonElement el = jsonObject.get(key);
        if (el.isJsonNull()) {
            return "";
        }
        JsonPrimitive p = el.getAsJsonPrimitive();
        return p.getAsString();
    }

    private Set<String> getIssueIds(String initialQuery) throws IOException, ClientException {
        byte[] htmlBytes = null;
        Path resultsHtmlPath = rootDir.resolve("metadata/"+project+"-results.html");
        if (Files.isRegularFile(resultsHtmlPath)) {
            System.out.println("relying on existing results html file: "+resultsHtmlPath);
            htmlBytes = Files.readAllBytes(resultsHtmlPath);
        } else {
            htmlBytes = HttpUtils.get(initialQuery);
            Files.write(resultsHtmlPath, htmlBytes);
        }
        String html = new String(htmlBytes, StandardCharsets.UTF_8);
        Matcher m = Pattern.compile("<tr id=\"b(\\d+)").matcher(html);
        Set set = new LinkedHashSet();
        while (m.find()) {
            set.add(m.group(1));
        }
        return set;
    }

}
