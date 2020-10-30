package org.tallison.bugs.bugzilla;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSyntaxException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.tallison.bugs.Attachment;
import org.tallison.bugs.ClientException;
import org.tallison.bugs.HttpUtils;
import org.tallison.bugs.ScraperUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;

class BugzillaWorker implements Callable<String> {

    private static String LIMIT = "&limit="; //how many results to bring back
    private static String OFFSET = "&offset="; //start at ...how far into the results to return.

    //restCGI always ends in /
    private String generalQueryByURL = "bug?" +
            //bring back id field and some other important metadata
            "include_fields=id,summary,keywords,product,component,creation_time,last_change_time" +
            "&order=bug_id%20DESC" + //order consistently by bug id desc -- completely arbitrary
            "&query_format=advanced" +
            "&o1=anywordssubstr" + //require any word to match
            "&f1=attachments.mimetype&v1="; //key words to match

    //if you want any issue with any type of attachment
    //&f1=attach_data.thedata&o1=isnotempty


    static Options OPTIONS = new Options()
            .addOption("u", "baseUrl", true, "base url")
            .addOption("o", "outputRoot", true, "directory to dump the data")
            .addOption("p", "project", true, "project name for the issue number labels 'POI' for POI-32132")
            .addOption("d", "product", true,
                    "optional specification of a product with a project, like POI")
            .addOption("m", "mimeMatch", true, "required: terms in the mime for matching")
            .addOption("k", "apiKey", true, "(optional) api key")
            .addOption("s", "size", true, "page result size")
            .addOption("i", "indexOnly", false,
                    "only grab the index/paging results, not the actual issues");

    int pageResultSize = 100;
    String restCGI = "rest.cgi/";

    private DateTimeFormatter formatter =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssVV");

    private String project;
    private String baseUrl;
    private Path metadataDir;
    private Path docsDir;
    private String mimeTypeStrings;
    private String product;//can be empty string
    private String apiKey;//can be empty string
    private boolean indexOnly = false;


    static final String[] POISON_COMMANDLINE = new String[0];
    private final ArrayBlockingQueue<String[]> commandlines;

    BugzillaWorker(ArrayBlockingQueue commandlines) {
        this.commandlines = commandlines;
    }

    @Override
    public String call() throws Exception {
        while (true) {
            String[] commandline = commandlines.poll();
            System.out.println("COMMANDLINE: " + Arrays.asList(commandline));
            if (commandline == POISON_COMMANDLINE) {
                return project;
            }
            processCommandline(commandline);

        }
    }

    private void processCommandline(String[] args) throws Exception {
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
        if (!baseUrl.endsWith("/")) {
            baseUrl += "/";
        }
        String mimeTypeStrings = commandLine.getOptionValue("m");//URLEncoder.encode(commandLine.getOptionValue("m"));
        Path outputDir = Paths.get(commandLine.getOptionValue("o"));
        String apiKey = "";
        if (commandLine.hasOption("k")) {
            apiKey = commandLine.getOptionValue("k");
        }
        String product = "";
        if (commandLine.hasOption("d")) {
            product = commandLine.getOptionValue("d");
        }

        reset(project, baseUrl, mimeTypeStrings, outputDir, apiKey, product);
        if (commandLine.hasOption("s")) {
            setPageResultSize(Integer.parseInt(commandLine.getOptionValue("s")));
        }
        if (commandLine.hasOption("i")) {
            setIndexOnly(true);
        }
        try {
            execute();
        } catch (Exception t) {
            System.err.println("fatal exception " + Arrays.toString(args));
            t.printStackTrace();
        }

    }

    private void reset(String project, String baseUrl,
                       String mimeTypeStrings, Path rootDir,
                       String apiKey, String product) {
        this.project = project;
        this.baseUrl = baseUrl;
        this.mimeTypeStrings = mimeTypeStrings;
        this.metadataDir = rootDir.resolve("metadata/" + project);
        this.docsDir = rootDir.resolve("docs/" + project);
        this.mimeTypeStrings = mimeTypeStrings;
        if (StringUtils.isBlank(apiKey)) {
            this.apiKey = "";
        } else {
            this.apiKey = "&api_key=" + apiKey;
        }
        this.product = product;
    }

    private void setIndexOnly(boolean indexOnly) {
        this.indexOnly = indexOnly;
    }

    private void setPageResultSize(int s) {
        this.pageResultSize = s;
    }

    private void execute() throws IOException, InterruptedException, ClientException {
        Files.createDirectories(docsDir);
        Files.createDirectories(metadataDir);

        int offset = 0;
        while (true) {
            List<String> issueIds = getIssueIds(offset, pageResultSize);
            if (issueIds.size() == 0 ) {
                break;
            }
            System.out.println("found issues " + project + " :: " + issueIds);
            System.out.println("\n\nnum issues: " + issueIds.size() +
                    " from offset " + offset);
            if (!indexOnly) {
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
            }
            offset += issueIds.size();
        }
    }

    private List<String> getIssueIds(int offset, int pageSize) throws IOException,
            ClientException, InterruptedException {
        Path resultsPath = metadataDir.resolve(offset + ".json.gz");
        byte[] bytes = null;
        if (Files.isRegularFile(resultsPath)) {
            bytes = gunzip(resultsPath);
        } else {
            String url = getIssueIdUrl(offset, pageSize);
            System.out.println("going to get issues: " + url);
            Path tmp = Files.createTempFile("bugzilla", "");
            try {
                System.err.println("trying: " + url);

                if (offset == 0) {
                    try {
                        HttpUtils.wget(url, tmp);
                    } catch (ClientException e) {
                        //try removing the .cgi
                        restCGI = "rest/";
                        url = getIssueIdUrl(offset, pageSize);
                        System.err.println("re-trying: " + url);
                        HttpUtils.wget(url, tmp);
                    }
                } else {
                    HttpUtils.wget(url, tmp);
                }
                bytes = Files.readAllBytes(tmp);
            } finally {
                Files.delete(tmp);
            }
            gz(resultsPath, bytes);
        }
        String json = new String(bytes, StandardCharsets.UTF_8);
        System.out.println(json);
        JsonElement root = JsonParser.parseString(json);
        if (!root.isJsonObject()) {
            return Collections.EMPTY_LIST;
        }
        JsonElement bugs = root.getAsJsonObject().get("bugs");
        if (bugs == null || bugs.isJsonNull()) {
            return Collections.EMPTY_LIST;
        }
        if (!bugs.isJsonArray()) {
            return Collections.EMPTY_LIST;
        }
        List<String> ids = new ArrayList<>();
        for (JsonElement idObj : bugs.getAsJsonArray()) {
            if (idObj.isJsonObject()) {
                String id = getAsString(idObj.getAsJsonObject(), "id");
                if (!StringUtils.isAllBlank(id)) {
                    ids.add(id);
                }
            }
        }
        return ids;
    }

    private String getIssueIdUrl(int offset, int pageSize) {

        String url = baseUrl + restCGI + generalQueryByURL + mimeTypeStrings +
                LIMIT + pageSize + OFFSET + offset;

        if (!StringUtils.isBlank(apiKey)) {
            url += "&api_key=" + apiKey;
        }

        if (!StringUtils.isBlank(product)) {
            url += "&product=" + product;
        }
        return url;
    }

    private boolean processIssue(String issueId) throws IOException, ClientException {
        Path jsonMetadataPath = metadataDir.resolve(project + "-" + issueId + ".json.gz");
        boolean networkCall = false;
        byte[] jsonBytes = null;
        System.err.println("going to get: " + jsonMetadataPath);
        if (Files.isRegularFile(jsonMetadataPath)) {
            System.err.println("getting from file: " + jsonMetadataPath);
            jsonBytes = gunzip(jsonMetadataPath);
        } else {
            String url = baseUrl + restCGI + "bug/" + issueId + "/attachment";
            if (!StringUtils.isBlank(apiKey)) {
                url += "?api_key=" + apiKey;
                if (!StringUtils.isBlank(product)) {
                    url += "&product=" + product;
                }
            } else if (!StringUtils.isBlank(product)) {
                url += "?product=" + product;
            }
            networkCall = true;
            Path tmp = Files.createTempFile("bugzilla", "");
            try {
                try {
                    HttpUtils.wget(url, tmp);
                } catch (InterruptedException | ClientException e) {
                    System.err.println("exception for " + project + " : " + issueId);
                    e.printStackTrace();
                    if (e.getMessage() != null && e.getMessage().contains("rate limited")) {
                        System.err.println("rate limited sleeping for two minutes");
                        try {
                            Thread.sleep(120000);
                        } catch (InterruptedException ex) {
                            ex.printStackTrace();
                        }
                        try {
                            HttpUtils.wget(url, tmp);
                        } catch (InterruptedException | ClientException e2) {
                            e.printStackTrace();
                            return networkCall;
                        }
                    } else {
                        return networkCall;
                    }
                }
                jsonBytes = Files.readAllBytes(tmp);
            } finally {
                Files.delete(tmp);
            }
            gz(jsonMetadataPath, jsonBytes);
        }
        JsonElement rootEl = null;
        try {
            rootEl = JsonParser.parseString(new String(jsonBytes, StandardCharsets.UTF_8));
        } catch (JsonSyntaxException e) {
            System.err.println("bad json: " + jsonMetadataPath);
            return networkCall;
        }
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
        String mime = getAsString(attachmentObj, "content_type");
        System.out.println(issueId + " " + i + " " + fileName + " mime: " + mime);
        Instant lastModified = ScraperUtils.getCreated(formatter, dateString);

        Attachment attachment = new Attachment("", fileName, lastModified);

        Path target = ScraperUtils.getInitialTarget(docsDir, attachment,
                project + "-" + issueId, i);
        if (Files.isRegularFile(target)) {
            return;
        }
        String data = getAsString(attachmentObj, "data");
        if (!StringUtils.isAllBlank(data)) {
            byte[] bytes = Base64.getDecoder().decode(data);
            try {
                Files.copy(new ByteArrayInputStream(bytes), target, StandardCopyOption.REPLACE_EXISTING);
                ScraperUtils.writeAttachment(target, docsDir, project + "-" + issueId, i, lastModified);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            System.err.println("empty data for: " + issueId + " " + i);
        }
    }

    private String getAsString(JsonObject jsonObject, String key) {
        if (jsonObject == null || !jsonObject.has(key)) {
            return "";
        }
        JsonElement el = jsonObject.get(key);
        if (el.isJsonNull()) {
            return "";
        }
        JsonPrimitive p = el.getAsJsonPrimitive();
        return p.getAsString();
    }

    /*
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
    */
    private void gz(Path p, byte[] bytes) throws IOException {
        try (OutputStream os = Files.newOutputStream(p)) {
            try (GzipCompressorOutputStream gz = new GzipCompressorOutputStream(os)) {
                gz.write(bytes);
                gz.flush();
            }
            os.flush();
        }
    }

    private byte[] gunzip(Path p) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (InputStream is = Files.newInputStream(p)) {
            try (GzipCompressorInputStream gz = new GzipCompressorInputStream(is)) {
                IOUtils.copy(gz, bos);
            }
        }
        return bos.toByteArray();
    }
}
