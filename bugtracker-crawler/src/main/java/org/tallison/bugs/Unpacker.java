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

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.tika.exception.TikaException;
import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.io.IOExceptionWithCause;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.pkg.PackageParser;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.util.HashSet;

import java.util.Set;
//to delete attachments, try: find -E . -regex '.*-[0-9]+.*-[0-9]+.*-[0-9]+.*' -delete
class Unpacker {

    PackageParser p = new PackageParser();
    static Set<String> PACKAGE_FORMAT_EXTS = new HashSet<>();
    static Set<String> COMPRESSED_FORMAT_EXTS = new HashSet<>();
    static {
        PACKAGE_FORMAT_EXTS.add(".zip");
        PACKAGE_FORMAT_EXTS.add(".7z");
        PACKAGE_FORMAT_EXTS.add(".tar");
        PACKAGE_FORMAT_EXTS.add(".gtar");
    }
    static {
        COMPRESSED_FORMAT_EXTS.add(".tgz");
        COMPRESSED_FORMAT_EXTS.add(".bz2");
        COMPRESSED_FORMAT_EXTS.add(".gz");
        COMPRESSED_FORMAT_EXTS.add(".gzip");
        COMPRESSED_FORMAT_EXTS.add(".xz");

    }
    public static void main(String[] args) throws Exception {
        Path dir = Paths.get(args[0]);
        Unpacker unpacker = new Unpacker();
        unpacker.processDir(dir);
    }

    private void processDir(Path dir) {
        for (File f : dir.toFile().listFiles()) {
            if (f.isDirectory()) {
                processDir(f.toPath());
            } else {
                processFile(f.toPath());
            }
        }
    }

    private void processFile(Path file) {
        //already unpacked, return
        if (file.getFileName().toString().contains("-\\d+.*?-\\d+.*-\\d+")) {
            System.err.println("skipping: "+file);
            return;
        }
        String ext = ScraperUtils.getExtension(file);
        if (COMPRESSED_FORMAT_EXTS.contains(ext)) {
            try {
                decompress(file);
            } catch (IOException e) {
                System.err.println(file);
                e.printStackTrace();
            }
            return;
        }
        if (!PACKAGE_FORMAT_EXTS.contains(ext)) {
            return;
        }

        Metadata m = new Metadata();
        try (TikaInputStream tis = TikaInputStream.get(file, m)) {
            ParseContext context = new ParseContext();
            context.set(EmbeddedDocumentExtractor.class, new FileEmbeddedDocumentExtractor(file));
            p.parse(tis, new DefaultHandler(), m, context);
        } catch (TikaException|IOException|SAXException| RuntimeException e) {
            if (e.getMessage() != null && e.getMessage().contains("Unable to unpack")) {
                //do nothing
            } else {
                System.err.println(file);
                e.printStackTrace();
            }
        }
    }

    private void decompress(Path file) throws IOException {
        CompressorStreamFactory factory = new CompressorStreamFactory();

        try (InputStream is = factory
                .createCompressorInputStream(TikaInputStream.get(file))) {
            Path tmp = Files.createTempFile("decompress-", "");
            Files.copy(is, tmp, StandardCopyOption.REPLACE_EXISTING);
            String extension = ScraperUtils.getExtension(tmp);
            //this handles the "tar" in tgz
            if (extension.equals(".tar") || extension.equals(".gtar")) {
                handleTar(file, tmp);
                return;
            }

            Path targ = file.getParent().resolve(file.getFileName().toString()
                    + "-" +
                    0
                    + extension);
            System.out.println("extracting "+file + " -> "+targ);
            Files.move(tmp, targ);
            Files.setLastModifiedTime(targ,Files.getLastModifiedTime(file));

        } catch (CompressorException e) {
            throw new IOExceptionWithCause(e);
        }
    }

    private void handleTar(Path rootFile, Path tmpTar) throws IOException {
        try (InputStream is = Files.newInputStream(tmpTar)) {
            try (TarArchiveInputStream tar = new TarArchiveInputStream(is)) {
                ArchiveEntry e = tar.getNextEntry();
                int count = 0;
                while (e != null) {
                    Path tmp = Files.createTempFile("tar-tmp", "");
                    Files.copy(tar, tmp, StandardCopyOption.REPLACE_EXISTING);
                    String extension = ScraperUtils.getExtension(tmp);
                    Path targ = rootFile.getParent().resolve(rootFile.getFileName().toString()
                            + "-" +
                            count++
                            + extension);
                    System.out.println("extracting "+rootFile + " -> "+targ);

                    Files.move(tmp, targ);
                    Files.setLastModifiedTime(targ,
                            FileTime.from(e.getLastModifiedDate().toInstant()));
                    e = tar.getNextEntry();
                }
            }
        } finally {
            Files.delete(tmpTar);
        }
    }


    private class FileEmbeddedDocumentExtractor
            implements EmbeddedDocumentExtractor {

        private int count = 0;
        private final Path rootFile;

        public FileEmbeddedDocumentExtractor(Path rootFile) {
            this.rootFile = rootFile;
        }


        public boolean shouldParseEmbedded(Metadata metadata) {
            return true;
        }

        public void parseEmbedded(InputStream inputStream, ContentHandler contentHandler, Metadata metadata, boolean outputHtml) throws SAXException, IOException {

            Path tmp = Files.createTempFile("unpacker", "");
            Files.copy(inputStream, tmp, StandardCopyOption.REPLACE_EXISTING);
            String extension = ScraperUtils.getExtension(tmp);
            Path targ = rootFile.getParent().resolve(rootFile.getFileName().toString()
                    + "-" +
                    count++
                    + extension);
            System.out.println("extracting " + rootFile + " -> " + targ);
            Files.move(tmp, targ);
            Files.setLastModifiedTime(targ, Files.getLastModifiedTime(rootFile));

        }

    }
}
