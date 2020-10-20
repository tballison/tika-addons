package org.tallison.bugs;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.tika.io.IOUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.Deflater;

/**
 * Simple class to zip files.
 *
 * Option 1: two arguments: root directory and file name regex
 * Option 2: three arguments: root directory, relative path list, output zip name
 *
 * Default behavior currently is to use only the name of the file and to
 * ignore the path to the file in the zip file.
 */
public class Zipper {

    public static void main(String[] args) throws Exception {
        Path rootDir = Paths.get(args[0]);
        if (args.length == 2) {
            Matcher m = Pattern.compile(args[1]).matcher("");
            Path zipFile = Paths.get(rootDir.getFileName().toString()+".zip");
            ZipArchiveOutputStream zip = new ZipArchiveOutputStream(zipFile.toFile());
            try {
                processDir(rootDir, rootDir, m, zip);
            } finally {
                zip.finish();
                zip.close();
            }

        } else if (args.length == 3){
            Path fileList = Paths.get(args[1]);
            Path zipFile = Paths.get(args[2]);
            ZipArchiveOutputStream zip = new ZipArchiveOutputStream(zipFile.toFile());
            zip.setLevel(Deflater.BEST_COMPRESSION);
            try (BufferedReader reader = Files.newBufferedReader(fileList, StandardCharsets.UTF_8)) {
                String line = reader.readLine();
                while (line != null) {
                    addEntry(rootDir, zip, rootDir.resolve(line.trim()));
                    line = reader.readLine();
                }

            } finally {
                zip.finish();
                zip.close();
            }
        }
    }


    private static void processDir(Path rootDir, Path dir, Matcher m, ZipArchiveOutputStream zip) throws IOException {
        for (File f : dir.toFile().listFiles()) {
            if (f.isDirectory()) {
                processDir(rootDir, f.toPath(), m, zip);
            } else {
                if (m.reset(f.getName()).find()) {
                    String rel = rootDir.relativize(f.toPath()).toString();
                    System.out.println("adding "+rel);
                    processFile(rootDir, f.toPath(), zip);
                } else {
                    System.err.println("skipping "+f.getName());
                }
            }
        }
    }

    private static void processFile(Path root, Path path,
                                    ZipArchiveOutputStream zip) throws IOException {
        addEntry(root, zip, path);
    }

    private static void addEntry(Path root, ZipArchiveOutputStream zip, Path path) throws IOException {
        ZipArchiveEntry entry = new ZipArchiveEntry(path.toFile(), root.relativize(path).toString());
        zip.putArchiveEntry(entry);
        try (InputStream is = Files.newInputStream(path)) {
            IOUtils.copy(is, zip);
        }
        zip.closeArchiveEntry();
    }
}
