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
package org.tallison.tika.client;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Property;
import org.apache.tika.metadata.serialization.JsonMetadataList;
import org.apache.tika.utils.ProcessUtils;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Simple tika client that kicks off a tika cli subprocess
 * for each input file. Probably not the fastest/most efficient, but it is robust.
 */
public class TikaClient {
    public static final String TIME_OUT = "timeout";
    public static final String SUCCESS = "success";
    public static final String SUCCESS_EXCEPTION = "success_exception";
    public static final String CRASHED = "crashed";


    public static final String TIKA_STATUS_PREFIX = "tika_status";
    public static Property EXIT_CODE = Property.externalInteger(TIKA_STATUS_PREFIX + ":exitValue");
    public static Property TIKA_STATUS = Property.externalClosedChoise(
            TIKA_STATUS_PREFIX + ":status",
            SUCCESS, SUCCESS_EXCEPTION, TIME_OUT, CRASHED);

    public static Property STACK_TRACE = Property.externalText(TIKA_STATUS_PREFIX +
                    ":stacktrace");

    long timeoutMS = 30000;//default timeout is 30 seconds

    public List<Metadata> parse(String[] args, TikaInputStream tis) throws IOException, InterruptedException {
        //TODO: figure out how to turn off logging or direct to file
        String[] newArgs = new String[args.length + 1];
        System.arraycopy(args, 0, newArgs, 0, args.length);
        newArgs[args.length] = ProcessUtils.escapeCommandLine(
                tis.getPath().toAbsolutePath().toString());
        ProcessBuilder pb = new ProcessBuilder(newArgs);
        pb.environment().put("LANG", "en_US.UTF-8");
        pb.environment().put("org.slf4j.simpleLogger.defaultLogLevel", "OFF");
        Process process = pb.start();
        StreamGobbler errGobbler = new ErrGobbler(process.getErrorStream());
        StreamGobbler inGobbler = new StreamGobbler(process.getInputStream());
        Thread errThread = new Thread(errGobbler);
        Thread inThread = new Thread(inGobbler);
        errThread.start();
        inThread.start();
        String status = SUCCESS;
        boolean completed = process.waitFor(timeoutMS, TimeUnit.MILLISECONDS);
        int exitValue = -1;
        try {
            exitValue = process.exitValue();
        } catch (IllegalThreadStateException e) {
            status = TIME_OUT;
        } finally {
            process.destroyForcibly();
        }

        Thread.sleep(1000);//wait for race condition writing to stdout/stderr
        inGobbler.stopGobblingAndDie();
        errGobbler.stopGobblingAndDie();

        String json = inGobbler.toString();
        String errString = errGobbler.toString().trim();
        List<Metadata> metadataList = null;
        try {
            metadataList = JsonMetadataList.fromJson(new StringReader(json));
        } catch (TikaException e) {
            //swallow
        } finally {
            if (metadataList == null && !TIME_OUT.equals(status)) {
                if (SUCCESS.equals(status) && !StringUtils.isAllBlank(errString)) {
                    status = SUCCESS_EXCEPTION;
                } else {
                    status = CRASHED;
                }
            }
        }
        return updateStatus(metadataList, errString, status, exitValue);
    }

    public void setTimeoutMS(long timeoutMS) {
        this.timeoutMS = timeoutMS;
    }

    private List<Metadata> updateStatus(List<Metadata> metadataList,
                                        String errString, String status, int exitValue) {
        List<Metadata> ret = metadataList;
        if (metadataList == null) {
            ret = new ArrayList<>();
        }

        Metadata parent = ret.size() > 0 ? ret.get(0) : new Metadata();
        if (!StringUtils.isAllBlank(errString.trim())) {
            status = SUCCESS_EXCEPTION;
            parent.set(STACK_TRACE, errString);
        }
        parent.set(TIKA_STATUS, status);
        parent.set(EXIT_CODE, exitValue);

        if (ret.size() == 0) {
            ret.add(parent);
        }

        return ret;
    }

    private class StreamGobbler implements Runnable {
        //plagiarized from org.apache.oodt's StreamGobbler
        protected final BufferedReader reader;
        protected boolean running = true;
        protected StringBuilder sb = new StringBuilder();

        private StreamGobbler(InputStream is) {
            this.reader = new BufferedReader(new InputStreamReader(new BufferedInputStream(is), UTF_8));
        }

        @Override
        public void run() {
            String line = null;
            try {
                while ((line = reader.readLine()) != null && running) {
                    sb.append(line).append("\n");
                }
            } catch (IOException e) {
                //swallow ioe
            }
        }

        private void stopGobblingAndDie() {
            running = false;
            IOUtils.closeQuietly(reader);
        }

        @Override
        public String toString() {
            return sb.toString();
        }
    }

    private class ErrGobbler extends StreamGobbler {
        boolean inStackTrace = false;

        private ErrGobbler(InputStream is) {
            super(is);
        }

        @Override
        public void run() {
            String line = null;
            try {
                while ((line = reader.readLine()) != null && running) {

                    if (line.startsWith("Exception in thread")) {
                        inStackTrace = true;
                    }
                    if (inStackTrace) {
                        sb.append(line).append("\n");
                    }
                }
            } catch (IOException e) {
                //swallow ioe
            }
        }
    }


}
