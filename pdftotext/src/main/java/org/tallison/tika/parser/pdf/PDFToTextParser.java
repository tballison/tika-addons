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
package org.tallison.tika.parser.pdf;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.xml.XMLConstants;

import org.apache.commons.io.IOUtils;
import org.apache.tika.config.Field;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.PDF;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.ContentHandlerDecorator;
import org.apache.tika.sax.OfflineContentHandler;
import org.apache.tika.sax.XHTMLContentHandler;
import org.apache.tika.utils.ProcessUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.tika.metadata.ParseStatus;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;


/**
 * Example parser wrapped around commandline utility pdftotext.
 *
 * NOTE: This parser does not handle embedded documents!
 *
 *
 */
public class PDFToTextParser extends AbstractParser {



    private static final Logger LOG = LoggerFactory.getLogger(
            PDFToTextParser.class);

    private static final MediaType MEDIA_TYPE = MediaType.application("pdf");

    private static final Set<MediaType> SUPPORTED_TYPES =
            Collections.singleton(MEDIA_TYPE);

    private static final int TIME_OUT_MILLIS = 60000;
    //after an exception or the parse has finished
    //how long to wait for the child process to shutdown
    private static final int WAIT_FOR_MILLIS = 20000;

    //This is the root of the path to write the verbose logs
    Path logRoot = null;

    public Set<MediaType> getSupportedTypes(ParseContext context) {
        return SUPPORTED_TYPES;
    }

    @Override
    public void parse(InputStream inputStream, ContentHandler contentHandler, Metadata metadata,
                      ParseContext parseContext) throws IOException, SAXException, TikaException {
        TemporaryResources tmp = new TemporaryResources();
        XHTMLContentHandler xhtml = new XHTMLContentHandler(contentHandler, metadata);
        List<String> errorMsgs = new ArrayList<>();


        try (Writer perFileLogWriter = getPerFileLogWriter(metadata)) {
            TikaInputStream tis = TikaInputStream.get(inputStream, tmp);
            Path txtFile = tmp.createTempFile();

            //tis.getPath() will return the actual file path if tis
            //was initialized with a File, or it will spool the file to disk
            errorMsgs = runPDFToText(tis.getPath(), txtFile);
            LOG.debug("error msg size: " + errorMsgs.size());
            logErrorMsgs(errorMsgs, perFileLogWriter);
            setStatusFlags(errorMsgs, metadata);
            if (errorMsgs.size() == 0) {
                try {
                    scrapeHTMLToTika(txtFile, xhtml, metadata);
                } catch (SAXException | IOException e) {
                    throw new TikaException("problem scraping" + e);
                }
            }
        } catch (Throwable t) {
            setStatusFlags(t, metadata);
        } finally {
            //if the file was spooled to disk, this will delete
            //the temp file.
            //And, of course, this will delete txtFile
            tmp.dispose();
            maybeThrowTikaException(errorMsgs);
        }
    }

    private Writer getPerFileLogWriter(Metadata metadata) throws IOException {
        if (logRoot == null) {
            return new EmptyWriter();
        }
        String fName = metadata.get(Metadata.RESOURCE_NAME_KEY);
        if (fName == null || fName.trim().length() == 0) {
            LOG.warn("couldn't find file name for logging!");
            return new EmptyWriter();
        }
        return Files.newBufferedWriter(
                logRoot.resolve(fName + ".log"), UTF_8);
    }

    private void setStatusFlags(Throwable t, Metadata metadata) {
        metadata.set(ParseStatus.SAFETY_STATUS,
                ParseStatus.SAFETY.UNSAFE.getName());
        metadata.set(ParseStatus.VALIDITY_STATUS,
                ParseStatus.VALIDITY.REJECTED.getName());
    }

    private void setStatusFlags(List<String> errorMsgs, Metadata metadata) {
        //TODO: do something smarter
        if (errorMsgs.size() > 0) {
            metadata.set(ParseStatus.SAFETY_STATUS,
                    ParseStatus.SAFETY.UNSAFE.getName());
            metadata.set(ParseStatus.VALIDITY_STATUS,
                    ParseStatus.VALIDITY.REJECTED.getName());
            for (String msg : errorMsgs) {
                metadata.add(ParseStatus.WARNINGS, msg);
            }
        } else {
            metadata.set(ParseStatus.SAFETY_STATUS,
                    ParseStatus.SAFETY.SAFE.getName());
            metadata.set(ParseStatus.VALIDITY_STATUS,
                    ParseStatus.VALIDITY.VALID.getName());
        }

    }

    private void logErrorMsgs(List<String> errorMsgs, Writer writer) throws IOException {
        for (String msg : errorMsgs) {
            writer.write(msg);
            writer.write("\n");
        }
    }


    @Field
    public void setLogRoot(String logRoot) {
        this.logRoot = Paths.get(logRoot);
    }

    public Path getLogRoot() {
        return logRoot;
    }

    private void maybeThrowTikaException(List<String> errorMsgs) throws TikaException {
        if (errorMsgs.size() == 0) {
            return;
        }
        LOG.debug("in maybe throw " + errorMsgs.size());
        StringBuilder sb = new StringBuilder();
        for (String err : errorMsgs) {
            sb.append(err).append("\n");
        }
        LOG.debug("throwing");
        throw new TikaException(sb.toString());
    }

    private List<String> runPDFToText(Path pdfFile, Path txtFile) throws IOException, TikaException {
        List<String> cmd = new ArrayList<>();
        cmd.add("pdftotext");
        cmd.add("-htmlmeta");
        cmd.add("-enc");
        cmd.add("UTF-8");
        cmd.add(ProcessUtils.escapeCommandLine(pdfFile.toAbsolutePath().toString()));
        cmd.add(ProcessUtils.escapeCommandLine(txtFile.toAbsolutePath().toString()));
        ProcessBuilder pb = new ProcessBuilder(cmd);
        pb.redirectError(ProcessBuilder.Redirect.PIPE);
        final Process process = pb.start();
        StreamGobbler outGobbler = new StreamGobbler("stdout", process.getInputStream());
        StreamGobbler errGobbler = new StreamGobbler("stderr", process.getErrorStream());

        Thread outGobblerThread = new Thread(outGobbler);
        outGobblerThread.start();

        Thread errGobblerThread = new Thread(errGobbler);
        errGobblerThread.start();

        boolean completed = false;
        try {
            completed = process.waitFor(TIME_OUT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new TikaException("PDFToTextParser interrupted", e);
        } finally {
            if (! completed) {
                process.destroyForcibly();
                throw new TikaException("timeout exception");
            }

            try {
                boolean stopped = process.waitFor(WAIT_FOR_MILLIS, TimeUnit.MILLISECONDS);
                if (! stopped) {
                    throw new TikaException("child process didn't stop after 10 seconds");
                }
                int status = process.exitValue();
                if (status != 0) {
                    throw new TikaException("Bad exit value: " + status);
                }
            } catch (InterruptedException e) {
                throw new TikaException("child process interrupted while shutting down", e);
            }
            try {
                outGobblerThread.join();
                errGobblerThread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return errGobbler.getLines();
    }

    private void scrapeHTMLToTika(Path txtFile,
                                  ContentHandler contentHandler, Metadata metadata)
            throws IOException, SAXException {
        // Parse the HTML document
        org.ccil.cowan.tagsoup.Parser parser =
                new org.ccil.cowan.tagsoup.Parser();


        parser.setContentHandler(
                new OfflineContentHandler(//be extra, extra cautious
                        new XHTMLDowngradeHandler(//remove html namespace markup
                                // scraper that actually processes output.
                                new PDFToTextScraper(metadata, contentHandler))));

        try (Reader reader = Files.newBufferedReader(txtFile, UTF_8)) {
            parser.parse(new InputSource(reader));
        }
    }

    /**
     * Class that scrapes the html output of pdftotext and puts
     * the metadata in Tika's metadata object and the text
     * where the text belongs.
     */
    private static class PDFToTextScraper extends ContentHandlerDecorator {
        private static final String HTML = "html";
        private static final String TITLE = "title";
        private static final String META = "meta";

        private static final String NAME = "name";
        private static final String CONTENT = "content";
        private static final String AUTHOR = "author";
        private static final String CREATOR = "creator";
        private static final String PRODUCER = "producer";
        private static final String CREATION_DATE = "creationdate";
        private final Metadata metadata;
        StringBuilder titleBuffer = new StringBuilder();
        boolean inTitle = false;

        public PDFToTextScraper(Metadata metadata, ContentHandler contentHandler) {
            super(contentHandler);
            this.metadata = metadata;
        }

        @Override
        public void startElement(
                String uri, String localName, String name, Attributes atts)
                throws SAXException {
            String lc = localName.toLowerCase(Locale.ENGLISH);
            if (lc.equals(TITLE)) {
                inTitle = true;
                return;
            } else if (lc.equals(META)) {
                handleMeta(atts);
                return;
            } else if (lc.equals(HTML) || lc.equals("body") || lc.equals("head")) {
                return;
            }
            super.startElement(XMLConstants.NULL_NS_URI, localName, name, atts);
        }

        @Override
        public void endElement(String uri, String localName, String name)
                throws SAXException {
            String lc = localName.toLowerCase(Locale.ENGLISH);
            if (lc.equals(META)) {
                return;
            }
            if (lc.equals(HTML) || lc.equals("body") || lc.equals("head")) {
                return;
            }
            if (lc.equals(TITLE)) {
                inTitle = false;
                metadata.set(TikaCoreProperties.TITLE, titleBuffer.toString());
                titleBuffer.setLength(0);
                return;
            }
            super.endElement(XMLConstants.NULL_NS_URI, localName, name);
        }

        @Override
        public void characters(char[] chars, int start, int len) throws SAXException {
            if (inTitle) {
                titleBuffer.append(chars, start, len);
            } else {
                super.characters(chars, start, len);
            }
        }

        private void handleMeta(Attributes atts) {
            String name = null;
            String content = null;
            for (int i = 0; i < atts.getLength(); i++) {
                String local = atts.getLocalName(i);
                if (local != null) {
                    String lc = local.toLowerCase(Locale.ENGLISH);
                    if (NAME.equals(lc)) {
                        name = atts.getValue(i);
                    } else if (CONTENT.equals(lc)) {
                        content = atts.getValue(i);
                    }
                }
            }
            if (isBlank(name) || isBlank(content)) {
                return;
            }
            name = name.toLowerCase(Locale.ENGLISH);
            if (AUTHOR.equals(name)) {
                metadata.add(TikaCoreProperties.CREATOR, content);
            } else if (CREATOR.equals(name)) {
                metadata.add(TikaCoreProperties.CREATOR_TOOL, content);
            } else if (PRODUCER.equals(name)) {
                metadata.add(PDF.DOC_INFO_PRODUCER, content);
            } else if (CREATION_DATE.equals(name)) {
                //TODO -- parse date and set it.
            }
        }

    }

    private static boolean isBlank(String s) {
        if (s == null || s.trim().length() == 0) {
            return true;
        }
        return false;
    }

    /**
     * Copied verbatim from org.apache.tika.parser.html.XHTMLDowngradeHandler
     *
     * Content handler decorator that downgrades XHTML elements to
     * old-style HTML elements before passing them on to the decorated
     * content handler. This downgrading consists of dropping all namespaces
     * (and namespaced attributes) and uppercasing all element names.
     * Used by the Tika's HtmlParser to make all incoming HTML look the same.
     */
    private static class XHTMLDowngradeHandler extends ContentHandlerDecorator {

        public XHTMLDowngradeHandler(ContentHandler handler) {
            super(handler);
        }

        @Override
        public void startElement(
                String uri, String localName, String name, Attributes atts)
                throws SAXException {
            String upper = localName.toUpperCase(Locale.ENGLISH);

            AttributesImpl attributes = new AttributesImpl();
            for (int i = 0; i < atts.getLength(); i++) {
                String auri = atts.getURI(i);
                String local = atts.getLocalName(i);
                String qname = atts.getQName(i);
                if (XMLConstants.NULL_NS_URI.equals(auri)
                        && !local.equals(XMLConstants.XMLNS_ATTRIBUTE)
                        && !qname.startsWith(XMLConstants.XMLNS_ATTRIBUTE + ":")) {
                    attributes.addAttribute(
                            auri, local, qname, atts.getType(i), atts.getValue(i));
                }
            }

            super.startElement(XMLConstants.NULL_NS_URI, upper, upper, attributes);
        }

        @Override
        public void endElement(String uri, String localName, String name)
                throws SAXException {
            String upper = localName.toUpperCase(Locale.ENGLISH);
            super.endElement(XMLConstants.NULL_NS_URI, upper, upper);
        }

        @Override
        public void startPrefixMapping(String prefix, String uri) {
        }

        @Override
        public void endPrefixMapping(String prefix) {
        }
    }

    private class StreamGobbler implements Runnable {
        //plagiarized from org.apache.oodt's StreamGobbler
        protected final BufferedReader reader;
        protected boolean running = true;
        List<String> lines = new ArrayList<>();
        private final String name;
        private StreamGobbler(String name, InputStream is) {
            this.reader = new BufferedReader(new InputStreamReader(
                    new BufferedInputStream(is), UTF_8));
            this.name = name;
        }

        @Override
        public void run() {
            String line = null;
            try {
                while ((line = reader.readLine()) != null && this.running) {
                    LOG.debug(name + " gobbling: " + line);
                    lines.add(line);
                }
            } catch (IOException e) {
                //swallow ioe
            }
        }

        private void stopGobblingAndDie() {
            running = false;
            IOUtils.closeQuietly(reader);
        }

        List<String> getLines() {
            return lines;
        }
    }


    public static class EmptyWriter extends Writer {
        @Override
        public void write(char[] cbuf, int off, int len) throws IOException {

        }

        @Override
        public void flush() throws IOException {

        }

        @Override
        public void close() throws IOException {

        }
    }
}
