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
package org.tallison.tika.rendering;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.IOUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import org.apache.tika.exception.TikaException;
import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.ToTextContentHandler;

public class TestPDFParser {
    static Parser AUTO_DETECT_PARSER = new AutoDetectParser();

    @Test
    public void testOne() throws Exception {
        Parser p = new AutoDetectParser();
        String fileName = "/test-documents/000010.pdf";
        ParseContext context = new ParseContext();
        Metadata metadata = new Metadata();
        ToTextContentHandler textContentHandler = new ToTextContentHandler();
        long start = System.currentTimeMillis();
        try (InputStream is =
                     TikaInputStream.get(TestPDFParser.class.getResourceAsStream(fileName))) {
            p.parse(is, textContentHandler, metadata, context);
        }
        long elapsed = System.currentTimeMillis() - start;
        System.out.println(elapsed + " : " + textContentHandler.toString());
    }

    @Test
    @Ignore
    public void testDirectory() throws IOException {
        Path inRoot = Paths.get("");
        Path outRoot = Paths.get("");
        Files.createDirectories(outRoot);
        long start = System.currentTimeMillis();
        processDir(inRoot, inRoot, outRoot);
        long elapsed = System.currentTimeMillis() - start;
    }

    private void processDir(Path inRoot, Path dir, Path outRoot) {
        for (File f : dir.toFile().listFiles()) {
            if (f.isDirectory()) {
                processDir(inRoot, f.toPath(), outRoot);
            } else {
                processFile(inRoot, f.toPath(), outRoot);
            }
        }
    }

    private void processFile(Path inRoot, Path path, Path outRoot) {
        ParseContext context = new ParseContext();
        Metadata metadata = new Metadata();
        ToTextContentHandler textContentHandler = new ToTextContentHandler();
        Path packageDir = outRoot.resolve(inRoot.relativize(path)+"-pkg");
        EmbeddedFileWriter embeddedDocumentExtractor =
                new EmbeddedFileWriter(packageDir);
        context.set(EmbeddedDocumentExtractor.class,
                embeddedDocumentExtractor);
        long start = System.currentTimeMillis();
        boolean success = false;
        try (InputStream is =
                     TikaInputStream.get(path)) {
            AUTO_DETECT_PARSER.parse(is, textContentHandler, metadata, context);
            success = true;
        } catch (Exception e) {
            //e.printStackTrace();
            //System.err.println(path.getFileName());
            //e.printStackTrace();
        }
        long elapsed = System.currentTimeMillis() - start;
        System.out.println(path.getFileName() + "\t" +
                embeddedDocumentExtractor.maxPage + "\t" + elapsed + "\t" + success);
    }

    private static class EmbeddedFileWriter implements EmbeddedDocumentExtractor {

        private final Path root;
        private int maxPage = 0;
        public EmbeddedFileWriter(Path root) {
            this.root = root;
        }

        @Override
        public boolean shouldParseEmbedded(Metadata metadata) {
            return true;
        }

        @Override
        public void parseEmbedded(InputStream inputStream, ContentHandler contentHandler,
                                  Metadata metadata, boolean b) throws SAXException, IOException {
            String pageNum = metadata.get(PDFBoxRenderer.PAGE_NUMBER);
            int pageInt = Integer.parseInt(pageNum);
            if (pageInt > maxPage) {
                maxPage = pageInt;
            }
            Path out = root.resolve("page-" + pageNum + ".png");
            Files.createDirectories(out.getParent());
            try (OutputStream os = Files.newOutputStream(out)) {
                IOUtils.copy(inputStream, os);
            }
        }

        public int getMaxPage() {
            return maxPage;
        }
    }

}
