package org.tallison.bugs;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Step5CpByExtension {

    public static void main(String[] args) throws IOException {
        Path source = Paths.get(args[0]);
        Path target = Paths.get(args[1]);
        Set<String> targetExtensions = new HashSet<>();/* Set.of(
                "aac",
                "ac3",
                "adb",
                "aea",
                "aif",
                "amr",
                "anm",
                "ape",
                "ar",
                "asf",
                "asm",
                "au",
                "avi",
                "bik",
                "caf",
                "der",
                "dnxhd",
                "dpx",
                "dss",
                "dts",
                "dv",
                "dxa",
                "exr",
                "ffmeta",
                "flac",
                "flv",
                "gdv",
                "h264",
                "hnm",
                "ico",
                "iso",
                "j2c",
                "jpf",
                "lha",
                "lvf",
                "m2ts",
                "m3u",
                "m3u8",
                "m4v",
                "mid",
                "mkv",
                "mlp",
                "mlv",
                "mmf",
                "mov",
                "movie",
                "mp4",
                "mp4a",
                "mpc",
                "mpd",
                "mpeg",
                "mpg",
                "mpga",
                "mts",
                "mtv",
                "mve",
                "mxf",
                "nsv",
                "nut",
                "obj",
                "ogg",
                "ogv",
                "ogx",
                "oma",
                "opus",
                "pbm",
                "pcm",
                "pcx",
                "pgm",
                "pls",
                "qmv",
                "qt",
                "qtx",
                "ram",
                "rl2",
                "rm",
                "san",
                "smk",
                "spec",
                "swf",
                "tga",
                "thd",
                "tivo",
                "torrent",
                "ts",
                "tta",
                "vc1",
                "vmd",
                "voc",
                "vp5",
                "vp6",
                "vpk",
                "vqf",
                "vtt",
                "wav",
                "webm",
                "wma",
                "wmv",
                "wv",
                "xbm",
                "xmv",
                "y4m",
                "yop"
                );*/
        targetExtensions.clear();
        processDirectory(source, source, target, targetExtensions);
        List<String> exts = new ArrayList<>(targetExtensions);
        Collections.sort(exts);
        for (String ext : exts) {
            System.out.println(ext);
        }
    }

    private static void processDirectory(Path srcBase, Path source, Path target,
                                         Set<String> targetExtensions) throws IOException {
        for (File f : source.toFile().listFiles()) {
            if (f.isDirectory()) {
                processDirectory(srcBase, f.toPath(), target, targetExtensions);
            } else {
                processFile(srcBase, f.toPath(), target, targetExtensions);
            }
        }
    }

    private static void processFile(Path srcBase, Path src, Path target,
                                    Set<String> targetExtensions) throws IOException {
        String n = src.getFileName().toString();
        int i = n.lastIndexOf(".");
        if (i > -1 && i < n.length()) {
            String ext = n.substring(i+1);
            if (ext.startsWith("zip")) {
                System.out.println(n + " : " + ext);
                return;
            }
            targetExtensions.add(ext);
            if (! targetExtensions.contains(ext)) {
                return;
            }
            return;
//            Path targ = target.resolve(srcBase.relativize(src));
  //          Files.createDirectories(targ.getParent());
    //        Files.copy(src, targ);
        }
    }
}
