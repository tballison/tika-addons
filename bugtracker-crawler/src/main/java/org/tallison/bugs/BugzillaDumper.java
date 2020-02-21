package org.tallison.bugs;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tika.mime.MediaType;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;

public class BugzillaDumper {
    public static void main(String[] args) throws Exception {
        File p = new File(args[0]);
        Map<String, Set<String>> mimeToExt = new TreeMap<>();
        try (Writer w = Files.newBufferedWriter(Paths.get(args[1]))) {
            w.write("Issue\tBugzillaMime\tBugzillaExtension\tTikaMime\tTikeExtension\tAttachmentName\n");
            for (File f : p.listFiles()) {
                processFile(f, w, mimeToExt);
            }
        }
        for (Map.Entry<String, Set<String>> e : mimeToExt.entrySet()) {
            String m = e.getKey();
            for (String ext : e.getValue()) {
                System.out.println(m+"\t"+ext);
            }
        }
    }

    private static void processFile(File f, Writer w, Map<String, Set<String>> mimeToExt) throws IOException  {
        try (InputStream is = new FileInputStream(f)) {
            try (Reader r = new InputStreamReader(new GZIPInputStream(is))) {
                JsonElement root = JsonParser.parseReader(r);
                JsonElement bugs = root.getAsJsonObject().get("bugs");

                if (bugs != null && bugs.isJsonObject()) {
                    int i = 0;
                    //should only be one issue=issueid, but why not iterate?
                    for (String k : bugs.getAsJsonObject().keySet()) {
                        JsonArray arr = bugs.getAsJsonObject().getAsJsonArray(k);
                        for (JsonElement el : arr) {
                            String filename = el.getAsJsonObject().get("file_name").getAsString();
                            String bugzillaExt = "."+FilenameUtils.getExtension(filename).trim().toLowerCase(Locale.US);
                            String bugzillaMime = el.getAsJsonObject().get("content_type").getAsString();
                            MediaType tikaMT = getMediaType(el.getAsJsonObject().get("data"));
                            String tikaExt = ScraperUtils.getExtension(tikaMT);
                            w.write(StringUtils.joinWith("\t",
                                    clean(f.getName().replaceAll(".json.gz", "")),
                                    clean(bugzillaMime),
                                    clean(bugzillaExt),
                                    clean(tikaMT.toString()),
                                    clean(tikaExt),
                                    clean(filename), "\n"));
                            Set<String> exts = mimeToExt.get(bugzillaMime);
                            if (exts == null) {
                                exts = new TreeSet<>();
                                mimeToExt.put(bugzillaMime, exts);
                            }
                            exts.add(bugzillaExt);
                        }
                    }
                }
            }
        }
    }

    private static MediaType getMediaType(JsonElement data) {
        if (data == null || data.isJsonNull()) {
            return MediaType.OCTET_STREAM;
        }
        byte[] bytes = Base64.getDecoder().decode(data.getAsString());
        Path tmp;
        try {
            tmp = Files.createTempFile("tmp", "");
        } catch (IOException e) {
            e.printStackTrace();
            return MediaType.OCTET_STREAM;
        }
        try {
            Files.write(tmp, bytes);
            return ScraperUtils.getMediaType(tmp);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
                try {
                    Files.delete(tmp);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        return MediaType.OCTET_STREAM;
    }


    private static String clean(String s) {
        s = s.replaceAll("[\r\n\t]", " ");
        return s;
    }
}
