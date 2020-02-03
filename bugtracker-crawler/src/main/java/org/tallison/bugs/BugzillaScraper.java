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
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.URLEncoder;
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
    /**
     * TODO: automatically figure out if the url needs .cgi?
     * TODO: consider gzipping .json issue files to save space
     */

    /*
        -p OOO -u https://bz.apache.org/ooo -m application -o /docs/ooo -s 10
        -p POI -u https://bz.apache.org/bugzilla/ -m application -o /docs/poi -d POI -s 10
        -p LIBRE_OFFICE -u https://bugs.documentfoundation.org/ -m application -o /Users/allison/Desktop/libre -s 10
        -p GHOSTSCRIPT -u https://bugs.ghostscript.com/ -m application -o /Users/allison/Desktop/ghostscript -s 10
     */

    private static String LIMIT = "&limit="; //how many results to bring back
    private static String OFFSET = "&offset="; //start at ...how far into the results to return.

    private String generalQueryByURL = "/bug?" +
            "include_fields=id" + //only bring back the id field
            "&order=bug_id%20DESC" + //order consistently by bug id desc -- completely arbitrary
            "&query_format=advanced" +
            "&o1=anywordssubstr" + //require any word to match
            "&f1=attachments.mimetype&v1="; //key words to match


    static Options OPTIONS = new Options()
                .addOption("u", "baseUrl", true, "base url")
                .addOption("o", "outputDir", true, "directory to dump the data")
                .addOption("p", "project", true, "project name for the issue number labels 'POI' for POI-32132")
                .addOption("d", "product", true,
                        "optional specification of a product with a project, like POI")
                .addOption("m", "mimeMatch", true, "required: terms in the mime for matching")
                .addOption("k", "apiKey", true, "(optional) api key")
                .addOption("s", "size", true, "page result size");

    int pageResultSize = 2;
    String restCGI = "rest.cgi/";

    private DateTimeFormatter formatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssVV");

    private final String project;
    private final String baseUrl;
    private final Path rootDir;
    private final String mimeTypeStrings;
    private final String product;//can be empty string
    private final String apiKey;//can be empty string

    public BugzillaScraper(String project, String baseUrl,
                           String mimeTypeStrings, Path path,
                           String apiKey, String product) {
        this.project = project;
        this.baseUrl = baseUrl;
        this.rootDir = path;
        this.mimeTypeStrings = mimeTypeStrings;
        if (StringUtils.isBlank(apiKey)) {
            this.apiKey = "";
        } else {
            this.apiKey = "&api_key=" + apiKey;
        }
        if (StringUtils.isBlank(product)) {
            this.product = "";
        } else {
            this.product = "&product="+apiKey;
        }
    }

    public static void main(String[] args) throws Exception {
        DefaultParser defaultCLIParser = new DefaultParser();
        CommandLine commandLine = null;
        try {
            commandLine = defaultCLIParser.parse(OPTIONS, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            //TODO: USAGE();
            return;
        }
        String project = commandLine.getOptionValue("p");
        String baseUrl = commandLine.getOptionValue("u");
        if (! baseUrl.endsWith("/")) {
            baseUrl += "/";
        }
        String mimeTypeStrings = URLEncoder.encode(commandLine.getOptionValue("m"));
        Path outputDir = Paths.get(commandLine.getOptionValue("o"));
        String apiKey = "";
        if (commandLine.hasOption("k")) {
            apiKey = commandLine.getOptionValue("k");
        }
        String product = "";
        if (commandLine.hasOption("d")) {
            product = commandLine.getOptionValue("d");
        }
        BugzillaScraper scraper = new BugzillaScraper(project,
                baseUrl, mimeTypeStrings, outputDir, apiKey, product);
        if (commandLine.hasOption("s")) {
            scraper.setPageResultSize(Integer.parseInt(commandLine.getOptionValue("s")));
        }
        scraper.execute();
    }

    private void setPageResultSize(int s) {
        this.pageResultSize = s;
    }

    private void execute() throws IOException, ClientException {
        Files.createDirectories(rootDir);
        Files.createDirectories(rootDir.resolve("metadata"));

        int offset = 0;
        while (true) {
            List<String> issueIds = getIssueIds(offset, pageResultSize);
            if (issueIds.size() == 0) {
                break;
            }
            System.out.println("found " + issueIds);
            System.out.println("\n\nnum issues: " + issueIds.size());
            boolean networkCall = false;
            for (String issueId : issueIds) {
                networkCall = processIssue(issueId);
                if (networkCall) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            offset += pageResultSize;
        }
    }

    private List<String> getIssueIds(int offset, int pageSize) throws ClientException {
        String url = getIssueIdUrl(offset, pageSize);
        byte[] bytes = null;
        if (offset == 0) {
            try {
                bytes = HttpUtils.get(url);
            } catch (ClientException e) {
                //try removing the .cgi
                restCGI = "rest/";
                url = getIssueIdUrl(offset, pageSize);
                bytes = HttpUtils.get(url);
            }
        } else {
            bytes = HttpUtils.get(url);
        }
        String json = new String(bytes, StandardCharsets.UTF_8);
        JsonElement root = JsonParser.parseString(json);
        if (! root.isJsonObject()) {
            return Collections.EMPTY_LIST;
        }
        JsonElement bugs = root.getAsJsonObject().get("bugs");
        if (bugs == null || bugs.isJsonNull()) {
            return Collections.EMPTY_LIST;
        }
        if (! bugs.isJsonArray()) {
            return Collections.EMPTY_LIST;
        }
        List<String> ids = new ArrayList<>();
        for (JsonElement idObj : bugs.getAsJsonArray()) {
            if (idObj.isJsonObject()) {
                String id = getAsString(idObj.getAsJsonObject(), "id");
                if (! StringUtils.isAllBlank(id)) {
                    ids.add(id);
                }
            }
        }
        return ids;
    }

    private String getIssueIdUrl(int offset, int pageSize) {
        String url = baseUrl+restCGI+generalQueryByURL+mimeTypeStrings +
                LIMIT+pageSize+OFFSET+offset;

        if (! StringUtils.isBlank(apiKey)) {
            url += "&api_key="+apiKey;
        }

        if (! StringUtils.isBlank(product)) {
            url += "&product="+product;
        }
        return url;
    }

    private boolean processIssue(String issueId) throws IOException, ClientException {
        Path jsonMetadataPath = rootDir.resolve("metadata/"+project+"-" + issueId + ".json");
        boolean networkCall = false;
        byte[] jsonBytes = null;
        if (Files.isRegularFile(jsonMetadataPath)) {
            jsonBytes = Files.readAllBytes(jsonMetadataPath);
        } else {
            String url = baseUrl+restCGI+"/bug/" + issueId + "/attachment";
            if (! StringUtils.isBlank(apiKey)) {
                url += "?api_key="+apiKey;
                if (! StringUtils.isBlank(product)) {
                    url += "&product="+product;
                }
            } else if (! StringUtils.isBlank(product)) {
                url += "?product="+product;
            }
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
            System.err.println("empty data for: "+issueId + " "+i);
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