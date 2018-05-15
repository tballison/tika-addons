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
package org.tallison.tika.parser.forkrecursive;

import org.apache.commons.compress.compressors.snappy.FramedSnappyCompressorOutputStream;
import org.apache.log4j.Logger;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.metadata.serialization.JsonMetadataList;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.RecursiveParserWrapper;
import org.apache.tika.sax.BasicContentHandlerFactory;
import org.apache.tika.utils.ExceptionUtils;
import org.xml.sax.helpers.DefaultHandler;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;
import java.util.zip.Checksum;

class TikaChildProcess implements Runnable, Checksum {


    private static final Logger LOGGER = Logger.getLogger(TikaChildProcess.class);
    private static final long DEFAULT_TIMEOUT_MILLIS = 60000;
    public static final String TIKA_FORKED_CHILD_TIMEOUT_PROP = "tika.forked.child.timeout";

    public static final byte ERROR = -1;

    public static final byte DONE = 0;

    public static final byte CALL = 1;

    public static final byte PING = 2;

    public static final byte RESOURCE = 3;

    public static final byte READY = 4;

    DataInputStream fromClient;
    DataOutputStream toClient;
    private boolean active = true;
    private final long timeout;
    final Parser parser;

    TikaChildProcess(InputStream is, OutputStream os, long timeout, Parser parser) {
        fromClient = new DataInputStream(
                new CheckedInputStream(is, this));
        toClient = new DataOutputStream(
                new CheckedOutputStream(os, this));
        this.timeout = timeout;
        this.parser = parser;

    }

    public static void main(String[] args) {
        long timeout = DEFAULT_TIMEOUT_MILLIS;
        if (System.getProperty(TIKA_FORKED_CHILD_TIMEOUT_PROP) != null) {
            timeout = Long.parseLong(System.getProperty(TIKA_FORKED_CHILD_TIMEOUT_PROP));
            LOGGER.debug("read " + timeout + " timeout milliseconds from the commandline");
        }

        Parser auto = new AutoDetectParser();
        Parser parser = new RecursiveParserWrapper(auto,
                new BasicContentHandlerFactory(BasicContentHandlerFactory.HANDLER_TYPE.TEXT, -1));

        TikaChildProcess server = new TikaChildProcess(System.in, System.out, timeout, parser);
        System.setIn(new ByteArrayInputStream(new byte[0]));
        System.setOut(System.err);

        Thread watchdog = new Thread(server, "Tika Watchdog");
        watchdog.setDaemon(true);
        watchdog.start();

        server.processRequests();
    }

    public void run() {
        try {
            LOGGER.info("Tika child process starting up");
            while (active) {
                active = false;
                Thread.sleep(timeout);
            }
            LOGGER.info("Tika child process shutting down");
            System.exit(0);
        } catch (InterruptedException e) {
        }
    }

    public void processRequests() {
        try {
            toClient.writeByte(READY);
            toClient.flush();

            while (true) {
                int request = fromClient.readByte();
                if (request == -1) {
                    break;
                } else if (request == PING) {
                    toClient.writeByte(PING);
                    toClient.flush();
                } else if (request == CALL) {
                    parse();
                } else if (request == DONE) {
                    return;
                } else {
                    throw new IllegalStateException("Unexpected request");
                }
            }
        } catch (Throwable t) {
            LOGGER.warn("Serious problem while processing requests", t);
        }
        System.err.flush();
    }

    private void parse() throws TikaException, IOException {
        Path path = Paths.get(fromClient.readUTF());
        List<Metadata> metadataList = null;
        Throwable t = null;
        try (TikaInputStream tis = TikaInputStream.get(path)) {
            parser.parse(tis, new DefaultHandler(), new Metadata(), new ParseContext());
        } catch (SecurityException e) {
            throw e;
        } catch (Exception e) {
            t = e;
        } finally {
            metadataList = new ArrayList<>(((RecursiveParserWrapper) parser).getMetadata());
            ((RecursiveParserWrapper) parser).reset();
            if (t != null) {
                if (metadataList == null) {
                    metadataList = new LinkedList<>();
                }
                Metadata m = null;
                if (metadataList.size() == 0) {
                    m = new Metadata();
                } else {
                    //take the top metadata item
                    m = metadataList.remove(0);
                }
                String stackTrace = ExceptionUtils.getFilteredStackTrace(t);
                m.add(TikaCoreProperties.TIKA_META_EXCEPTION_PREFIX + "runtime", stackTrace);
                metadataList.add(0, m);
            }
        }

        toClient.writeByte(READY);
        toClient.flush();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        Writer writer = new OutputStreamWriter(new FramedSnappyCompressorOutputStream(bos), StandardCharsets.UTF_8);
        JsonMetadataList.toJson(metadataList, writer);
        writer.flush();
        writer.close();
        byte[] bytes = bos.toByteArray();
        toClient.writeInt(bytes.length);
        toClient.flush();
        toClient.write(bytes, 0, bytes.length);
        toClient.flush();
    }

    @Override
    public void update(int b) {
        active = true;
    }

    @Override
    public void update(byte[] b, int off, int len) {
        active = true;
    }

    @Override
    public long getValue() {
        return 0;
    }

    @Override
    public void reset() {

    }
}
