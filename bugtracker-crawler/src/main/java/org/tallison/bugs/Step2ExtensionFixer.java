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

import org.apache.commons.exec.util.MapUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.tika.mime.MediaType;
import org.tallison.bugs.utils.MapUtil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Runs detection on a file and will change its extension
 * if the extension appears to be wrong according to Tika's detection.
 */
public class Step2ExtensionFixer {
    static Set<MediaType> UNRELIABLE_MEDIA_TYPES = new HashSet<>();
    static Set<String> DONT_CHANGE_ORIG_EXT = new HashSet<>();
    static Map<String, String> CUSTOM_TIKA_MAPPINGS = new HashMap<>();

    static {
        //don't trust Tika if it detects these types
        UNRELIABLE_MEDIA_TYPES.add(MediaType.application("mbox"));
        UNRELIABLE_MEDIA_TYPES.add(MediaType.parse("message/rfc822"));
        UNRELIABLE_MEDIA_TYPES.add(MediaType.text("plain"));
        UNRELIABLE_MEDIA_TYPES.add(MediaType.text("x-matlab"));
        UNRELIABLE_MEDIA_TYPES.add(MediaType.OCTET_STREAM);
    }

    static {
        //coz tika does this in 2 steps, leave as is
        DONT_CHANGE_ORIG_EXT.add(".tgz");
        //these are incorrectly recognized by Tika
        //TODO: fix these in Tika
        DONT_CHANGE_ORIG_EXT.add(".eps");
        DONT_CHANGE_ORIG_EXT.add(".spl");
        DONT_CHANGE_ORIG_EXT.add(".prn");
        DONT_CHANGE_ORIG_EXT.add(".otf");
        DONT_CHANGE_ORIG_EXT.add(".pfb");
        DONT_CHANGE_ORIG_EXT.add(".oxps");
        DONT_CHANGE_ORIG_EXT.add(".oxt");//zip
        DONT_CHANGE_ORIG_EXT.add(".cdr");//zip
        DONT_CHANGE_ORIG_EXT.add(".odt");
        DONT_CHANGE_ORIG_EXT.add(".ods");
        DONT_CHANGE_ORIG_EXT.add(".odg");//odp
        DONT_CHANGE_ORIG_EXT.add(".jar");//zip
        DONT_CHANGE_ORIG_EXT.add(".cbz");//zip
        DONT_CHANGE_ORIG_EXT.add(".odp");
        DONT_CHANGE_ORIG_EXT.add(".exe");
        DONT_CHANGE_ORIG_EXT.add(".xcu");//xml
        DONT_CHANGE_ORIG_EXT.add(".nbm");
        DONT_CHANGE_ORIG_EXT.add(".xdl");
        DONT_CHANGE_ORIG_EXT.add(".svg");
        DONT_CHANGE_ORIG_EXT.add(".fodt"); //xml
        DONT_CHANGE_ORIG_EXT.add(".fods");
        DONT_CHANGE_ORIG_EXT.add(".fodg");//xml
        DONT_CHANGE_ORIG_EXT.add(".fodp");//xml
        DONT_CHANGE_ORIG_EXT.add(".py"); //recognized as .sh
        DONT_CHANGE_ORIG_EXT.add(".rb"); //recognized as .sh
        DONT_CHANGE_ORIG_EXT.add(".xhp"); //xml
        DONT_CHANGE_ORIG_EXT.add(".mml"); //xml
        DONT_CHANGE_ORIG_EXT.add(".fdx"); //xml
        DONT_CHANGE_ORIG_EXT.add(".xba"); //xml
        DONT_CHANGE_ORIG_EXT.add(".sog"); //xml
        DONT_CHANGE_ORIG_EXT.add(".soe"); //xml
        DONT_CHANGE_ORIG_EXT.add(".soc"); //xml
        DONT_CHANGE_ORIG_EXT.add(".sod"); //xml
        DONT_CHANGE_ORIG_EXT.add(".ui"); //xml
        DONT_CHANGE_ORIG_EXT.add(".pps"); //ppt
        DONT_CHANGE_ORIG_EXT.add(".pot");//ppt
        DONT_CHANGE_ORIG_EXT.add(".ppsx"); //pptx
        DONT_CHANGE_ORIG_EXT.add(".exe"); //dll
        DONT_CHANGE_ORIG_EXT.add(".key"); //zip
        DONT_CHANGE_ORIG_EXT.add(".pages"); //zip
        DONT_CHANGE_ORIG_EXT.add(".ogv");//loses information -> ogx
        DONT_CHANGE_ORIG_EXT.add(".ogg");//loses information -> ogx
        DONT_CHANGE_ORIG_EXT.add(".vdx");//xml
        DONT_CHANGE_ORIG_EXT.add(".abw");//xml
        DONT_CHANGE_ORIG_EXT.add(".xcd");//xml
        DONT_CHANGE_ORIG_EXT.add(".xconf");//xml config file for Apache FOP
        DONT_CHANGE_ORIG_EXT.add(".xpi");//zip
        DONT_CHANGE_ORIG_EXT.add(".xul");//xml
        DONT_CHANGE_ORIG_EXT.add(".plist");//xml or binary
        DONT_CHANGE_ORIG_EXT.add(".ai");//adobe illustrator otherwise automatically detected as pdf
        DONT_CHANGE_ORIG_EXT.add(".war");
        DONT_CHANGE_ORIG_EXT.add(".bau");//libre office something or other, zip
        //to add  ott scc sci fsxw -> zip
        //fsxw -> xml
        //odt -> ott
        //odm -> otm

    }

    static {
        CUSTOM_TIKA_MAPPINGS.put(".gtar", ".tar");
    }

    public static void main(String[] args) {
        Path dir = Paths.get(args[0]);
        boolean dryRun = false;
        if (args.length > 1 && args[1].
                toLowerCase(Locale.US).contains("dryrun")) {
            dryRun = true;
        }
        Map<String, TokenCounts> fromTo = new HashMap<>();
        process(dir, fromTo, dryRun);

        for (Map.Entry<String, TokenCounts> from : ((Map<String, TokenCounts>)MapUtil.sortByDescendingValue(fromTo)).entrySet()) {
            TokenCounts tk = from.getValue();
            for (Map.Entry<String, MutableInt> e : MapUtil.sortByDescendingValue(tk.tokens).entrySet()) {
                System.out.println(from.getKey()+"\t->\t"+e.getKey()+"\t"+e.getValue().intValue());
            }
        }
    }

    private static void process(Path path, Map<String, TokenCounts> fromTo, boolean dryRun) {
        if (Files.isDirectory(path)) {
            processDir(path, fromTo, dryRun);
        } else {
            try {
                processFile(path, fromTo, dryRun);
            } catch (IOException e) {
                System.err.println(path);
                e.printStackTrace();
            }
        }
    }

    private static void processFile(Path path, Map<String, TokenCounts> fromTo, boolean dryRun) throws IOException {
        String origExt = FilenameUtils.getExtension(path.getFileName().toString());
        //FilenameUtils does not include the initial ".", but Tika does.
        //we assume the initial "." from here on...
        if (!StringUtils.isAllBlank(origExt)) {
            origExt = "."+origExt;
        }
        if (DONT_CHANGE_ORIG_EXT.contains(origExt)) {
            return;
        }
        String normedExt = origExt;
        if (StringUtils.isBlank(origExt) || origExt.length() > 6) {
            normedExt = "";
        }
        normedExt = normedExt.toLowerCase(Locale.US);

        MediaType mt = ScraperUtils.getMediaType(path);
        String tikaExt = "";
        if (! UNRELIABLE_MEDIA_TYPES.contains(mt)) {
            tikaExt = ScraperUtils.getExtension(mt);
            if (CUSTOM_TIKA_MAPPINGS.containsKey(tikaExt)) {
                tikaExt = CUSTOM_TIKA_MAPPINGS.get(tikaExt);
            }
        }

        String newExt = "";
        if (! StringUtils.isBlank(tikaExt)) {
            newExt = tikaExt;
        } else {
            newExt = normedExt;
        }
        //override Tika if the lowercased version
        //of the original should not be changed
        if (DONT_CHANGE_ORIG_EXT.contains(normedExt)) {
            newExt = normedExt;
        }
        if (newExt.equals(origExt) || StringUtils.isAllBlank(newExt) ||
            newExt.equals(".")) {
            return;
        }
        TokenCounts tk = fromTo.get(origExt);
        if (tk == null) {
            tk = new TokenCounts();
            fromTo.put(origExt, tk);
        }
        tk.increment(newExt);
        String fileName = path.getFileName().toString();
        String newFileName = fileName.substring(0, fileName.length()-origExt.length());
        newFileName += newExt;

        Path targ = path.getParent().resolve(newFileName);
        //If there isn't string equality in the names, then on some OS,
        //the files are different e.g. 1.pdf and 1.PDF.
        if (path.getFileName().toString().equals(newFileName) && Files.exists(targ)) {
            System.err.println("targ file already exists: "+targ +
                    " from "+path);
            return;
        }
        System.out.println("mv "+path.toAbsolutePath() + " -> "+targ.toAbsolutePath());
        if (! dryRun) {
            Files.move(path, targ, StandardCopyOption.ATOMIC_MOVE);
        }
    }

    private static void processDir(Path path, Map<String, TokenCounts> fromTo, boolean dryRun) {
        for (File f : path.toFile().listFiles()) {
            process(f.toPath(), fromTo, dryRun);
        }
    }

    private static class TokenCounts implements Comparable {
        Map<String, MutableInt> tokens = new HashMap<>();
        int count = 0;
        void increment(String token) {
            MutableInt cnt = tokens.get(token);
            if (cnt == null) {
                cnt = new MutableInt(1);
                tokens.put(token, cnt);
            } else {
                cnt.increment();
            }
            count++;
        }

        @Override
        public int compareTo(Object o) {
            return Integer.compare(this.count, ((TokenCounts)o).count);
        }
    }
}
