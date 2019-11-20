package org.tallison.tikaeval.example;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.tika.metadata.Metadata;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ESClient implements SearchClient {

    private static final String _ID = "_id";
    private static final String _DOC = "_doc";
    private static final Gson GSON = new Gson();

    private final CloseableHttpClient httpClient;
    private final JsonParser parser = new JsonParser();
    private final String url;//must include esbase and es collection; must end in /
    private final String esBase;
    private final String esCollection;

    public ESClient(String url) {
        httpClient = HttpClients.createDefault();
        String tmp = url;
        if (!url.endsWith("/")) {
            tmp = tmp + "/";
        }
        this.url = tmp;
        String base = tmp.substring(0, tmp.length() - 1);
        int indexOf = base.lastIndexOf("/");
        if (indexOf < 0) {
            throw new IllegalArgumentException("can't find / before collection name; " +
                    "should be, e.g.: http://localhost:9200/my_collection");
        }
        this.esBase = base.substring(0, indexOf + 1);
        this.esCollection = base.substring(indexOf + 1);

    }

    @Override
    public void deleteAll() throws IOException {
        Map<String, Object> q = wrapAMap("query",
                wrapAMap("match_all", Collections.EMPTY_MAP));
        JsonResponse response = postJson(url + "_delete_by_query", GSON.toJson(q));
        if (response.getStatus() != 200) {
            throw new IOException(response.getMsg());
        }
    }

    @Override
    public void addDoc(Metadata metadata) throws IOException {
        StringBuilder sb = new StringBuilder();
        Map<String, Object> fields = new HashMap<>();
        for (String n : metadata.names()) {
            String[] vals = metadata.getValues(n);
            if (vals.length == 1) {
                fields.put(n, vals[0]);
            } else {
                fields.put(n, vals);
            }
        }
        String id = (String) fields.remove(_ID);
        String indexJson = getBulkIndexJson(id);
        sb.append(indexJson).append("\n");
        sb.append(GSON.toJson(fields)).append("\n");
        JsonResponse response = postJson(url + "/_bulk", sb.toString());
        if (response.getStatus() != 200) {
            throw new IOException(response.getMsg());
        }
    }

    private String getBulkIndexJson(String id) {
        JsonObject innerObject = new JsonObject();
        innerObject.add("_type", new JsonPrimitive(_DOC));
        innerObject.add(_ID, new JsonPrimitive(id));
        JsonObject outerObject = new JsonObject();
        outerObject.add("index", innerObject);
        return outerObject.toString();
    }

    protected JsonResponse postJson(String url, String json) throws IOException {
        HttpPost httpRequest = new HttpPost(url);
        ByteArrayEntity entity = new ByteArrayEntity(json.getBytes(StandardCharsets.UTF_8));
        httpRequest.setEntity(entity);
        httpRequest.setHeader("Accept", "application/json");
        httpRequest.setHeader("Content-type", "application/json; charset=utf-8");
        //this was required because of connection already bound exceptions on windows :(
        //httpPost.setHeader("Connection", "close");

        //try (CloseableHttpClient httpClient = HttpClients.createDefault()) {

        try (CloseableHttpResponse response = httpClient.execute(httpRequest)) {
            int status = response.getStatusLine().getStatusCode();
            if (status == 200) {
                try (Reader reader = new BufferedReader(
                        new InputStreamReader(response.getEntity().getContent(),
                                StandardCharsets.UTF_8))) {
                    JsonElement element = parser.parse(reader);
                    return new JsonResponse(200, element);
                }
            } else {
                return new JsonResponse(status,
                        new String(EntityUtils.toByteArray(response.getEntity()),
                                StandardCharsets.UTF_8));
            }
        } finally {
            httpRequest.releaseConnection();
        }
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
    }

    //list: String, Object, String, Object,
    //where the String is the key and the object is the value
    private Map<String, Object> wrapAMap(Object... args) {
        Map<String, Object> ret = new HashMap<>();
        for (int i = 0; i < args.length - 1; i += 2) {
            String key = (String) args[i];
            Object value = args[i + 1];
            ret.put(key, value);
        }
        return ret;
    }


    @Override
    public String getIdField() {
        return "_id";
    }
}
