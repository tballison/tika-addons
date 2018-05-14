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

import org.apache.log4j.Logger;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.serialization.JsonMetadataList;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.tallison.tika.parser.forkrecursive.TikaChildProcess.PING;
import static org.tallison.tika.parser.forkrecursive.TikaChildProcess.READY;

class TikaClient implements AutoCloseable {
    private static final Logger LOGGER = Logger.getLogger(TikaClient.class);

    private Process childProcess;
    private DataOutputStream toChild;
    private DataInputStream fromChild;


    TikaClient(List<String> java) throws FatalTikaClientException {
        startServer(Collections.unmodifiableList(java));
        try {
            byte b = fromChild.readByte();
            if (b != READY) {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                try {
                    while (b != -1) {
                        bos.write(b);
                        b = fromChild.readByte();
                    }
                } catch (EOFException e) {
                    //swallow
                }
                throw new FatalTikaClientException("couldn't start child process: " +
                        new String(bos.toByteArray(), StandardCharsets.UTF_8));
            }
        } catch (IOException e) {
            LOGGER.warn(e);
            throw new FatalTikaClientException("IO exception while trying to start child process", e);
        }
    }

    public List<Metadata> parse(Path file) throws TikaException {

        try {
                toChild.writeByte(TikaChildProcess.CALL);
                toChild.flush();
                toChild.writeUTF(file.toString());
                toChild.flush();
                String json = null;

                int n = fromChild.readByte();
                if (n == READY) {
                    json = fromChild.readUTF();
                } else {
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    try {
                        while (n != -1) {
                            bos.write(n);
                            n = fromChild.readByte();
                        }
                    } catch (EOFException e) {
                        //swallow
                    }
                    throw new FatalTikaParseException("expected ready: "+new String(bos.toByteArray(), StandardCharsets.UTF_8));
                }
                try (Reader reader = new StringReader(json)) {
                             //new InputStreamReader(new ByteArrayInputStream(bytes), StandardCharsets.UTF_8)) {
                    return JsonMetadataList.fromJson(reader);
                }
        } catch (IOException e) {
            throw new FatalTikaParseException("serious problem with parser", e);
        }
    }

    private void startServer(List<String> java) throws FatalTikaClientException {
        List<String> command = new ArrayList<>();
        command.addAll(java);
        command.add("org.tallison.tika.parser.forkrecursive.TikaChildProcess");
        ProcessBuilder builder = new ProcessBuilder();
        builder.command(command);
        builder.redirectOutput(ProcessBuilder.Redirect.PIPE);
        builder.redirectInput(ProcessBuilder.Redirect.PIPE);
        builder.redirectError(ProcessBuilder.Redirect.INHERIT);
        try {
            childProcess = builder.start();
        } catch (IOException e) {
            LOGGER.warn(e);
            throw new FatalTikaClientException("couldn't start server", e);
        }
        fromChild = new DataInputStream(childProcess.getInputStream());
        toChild = new DataOutputStream(childProcess.getOutputStream());
    }

    public synchronized void close() {
        if (childProcess == null) {
            return;
        }
        LOGGER.info("shutting down server process");
        childProcess.destroyForcibly();
    }

    public synchronized boolean ping() {
        try {
            toChild.writeByte(PING);
            toChild.flush();

            while (true) {
                int type = fromChild.readByte();
                if (type == PING) {
                    return true;
                } else {
                    return false;
                }
            }
        } catch (IOException e) {
            return false;
        }
    }
}