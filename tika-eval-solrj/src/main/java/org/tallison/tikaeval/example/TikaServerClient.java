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
package org.tallison.tikaeval.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.IOUtils;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.serialization.JsonMetadataList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client for tika-server
 */
public class TikaServerClient implements TikaClient {

    enum INPUT_METHOD {
        FILE,
        INPUTSTREAM
    }

    private static final Logger LOG = LoggerFactory.getLogger(TikaServerClient.class);

    private final static String END_POINT = "/rmeta/text";
    private final static int MAX_TRIES = 3;
    private final static int TIMEOUT_SECONDS = 360; // seconds

    //if there are retries, what is the maximum amount of millis
    private final static long MAX_TIME_FOR_RETRIES_MILLIS = 30000;

    //how long to sleep each time if you couldn't connect to
    //the tika-server
    private final static long SLEEP_ON_NO_CONNECTION_MILLIS = 1000;

    private final HttpClient client;
    private final List<String> urls;
    private final INPUT_METHOD inputMethod;
    private enum STATE {
        NO_RESPONSE_IO_EXCEPTION,
        RESPONSE_NON_200_STATUS,
        RESPONSE_BAD_REQUEST,
        RESPONSE_PARSE_EXCEPTION,
        RESPONSE_UNPROCESSABLE_EXCEPTION,
        RESPONSE_SERVER_UNAVAILABLE,
        RESPONSE_BAD_ENTITY,
        RESPONSE_SUCCESS,
        INTERRUPTED_EXCEPTION;
    }

    public TikaServerClient(INPUT_METHOD inputMethod, String ... urls) {
        this.inputMethod = inputMethod;
        RequestConfig config = RequestConfig.custom()
                .setConnectTimeout(TIMEOUT_SECONDS * 1000)
                .setConnectionRequestTimeout(TIMEOUT_SECONDS * 1000)
                .setSocketTimeout(TIMEOUT_SECONDS * 1000).build();
        this.client = HttpClients.custom().setDefaultRequestConfig(config).build();
        this.urls = new ArrayList<>();
        for (String u : urls) {
            this.urls.add(u + END_POINT);
        }
    }

    @Override
    public List<Metadata> parse(String fileKey, TikaInputStream tis)
            throws IOException, TikaClientException {

        TikaServerResponse response = tryParse(fileKey, tis);

        int tries = 1;
        long retryStart = System.currentTimeMillis();
        long retryElapsed = 0;
        while (response.state !=
                STATE.RESPONSE_PARSE_EXCEPTION &&
                response.state != STATE.RESPONSE_SUCCESS
                && tries++ < MAX_TRIES
                && retryElapsed < MAX_TIME_FOR_RETRIES_MILLIS) {
            LOG.warn(String.format(Locale.US,
                    "Retrying '%s' because of %s",
                    fileKey,
                    response.state.toString()));
            response = tryParse(fileKey, tis);
            //if there was no connection to the server,
            //decrement the tries and wait a bit before trying again
            if (response.state == STATE.NO_RESPONSE_IO_EXCEPTION) {
                tries--;
                try {
                    Thread.sleep(SLEEP_ON_NO_CONNECTION_MILLIS);
                } catch (InterruptedException e) {
                    throw new TikaClientException("interrupted", e);
                }
            }
            retryElapsed = System.currentTimeMillis() - retryStart;
        }
        if (response.state == STATE.RESPONSE_PARSE_EXCEPTION ||
            response.state == STATE.RESPONSE_SUCCESS) {
            return response.metadataList;
        }
        throw new TikaClientException(
                String.format(Locale.US,
                        "Couldn't parse file (%s); state=%s",
                        fileKey,
                        response.state.toString()));
    }

    private TikaServerResponse tryParse(String fileKey, TikaInputStream is) throws IOException {
        int index = ThreadLocalRandom.current().nextInt(urls.size());
        HttpPut put = new HttpPut(urls.get(index));
        if (inputMethod.equals(INPUT_METHOD.INPUTSTREAM)) {
            put.setEntity(new InputStreamEntity(is));
        } else {
            String p = is.getPath().toUri().toString();
            put.setHeader("fileUrl", p);
        }
        return getResponse(fileKey, put);
    }

    private TikaServerResponse tryParse(String fileKey, Path path) {
        int index = ThreadLocalRandom.current().nextInt(urls.size());
        HttpPut put = new HttpPut(urls.get(index));
        String p = path.toUri().toString();
        put.setHeader("fileUrl", p);
        return getResponse(path.toAbsolutePath().toString(), put);
    }

    private TikaServerResponse getResponse(String fileKey, HttpPut put) {
        long start = System.currentTimeMillis();
        HttpResponse response = null;
        try {
            response = client.execute(put);
            LOG.info("took "+(System.currentTimeMillis()-start) +" to get response");
        } catch (IOException e) {
            LOG.warn("couldn't connect to tika-server", e);
            //server could be offline or a bad url
            return new TikaServerResponse(STATE.NO_RESPONSE_IO_EXCEPTION);
        }
        int statusInt = response.getStatusLine().getStatusCode();
        if (statusInt == 200 || statusInt == 422) {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(response.getEntity().getContent(),
                    StandardCharsets.UTF_8))) {
                return new TikaServerResponse(STATE.RESPONSE_SUCCESS,
                        -1, "", JsonMetadataList.fromJson(reader));
            } catch (IOException | TikaException e) {
                LOG.warn("couldn't read http entity", e);
                return new TikaServerResponse(STATE.RESPONSE_BAD_ENTITY,
                        200, "", null);
            }
        } else if (statusInt == 415) {
            LOG.warn("unsupported media type: "+fileKey);
            //unsupported media type
            return new TikaServerResponse(STATE.RESPONSE_UNPROCESSABLE_EXCEPTION);
        } /*else if (statusInt == 422) {
            LOG.warn("parse exception: "+path);
            return new TikaServerResponse(STATE.RESPONSE_PARSE_EXCEPTION);
        }*/ else if (statusInt == 400) {
            LOG.warn("bad request: " + fileKey);
            return new TikaServerResponse(STATE.RESPONSE_BAD_REQUEST);
        } else if (statusInt == 503) {
            LOG.warn("server active, but not available: "+fileKey);
            //server is in shutdown mode
            return new TikaServerResponse(STATE.RESPONSE_SERVER_UNAVAILABLE);
        }
        String msg = null;
        try {
            msg = IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8.toString());
        } catch (IOException e) {
            //swallow
        }
        return new TikaServerResponse(STATE.RESPONSE_NON_200_STATUS, statusInt, msg);
    }

    private static class TikaServerResponse {

        private final STATE state;
        private final int status;
        private final String responseMsg;
        private final List<Metadata> metadataList;

        TikaServerResponse(STATE state) {
            this(state, -1, null, null);
        }

        TikaServerResponse(STATE state, int status, String responseMsg) {
            this(state, status, responseMsg, null);
        }

        TikaServerResponse(STATE state, int status,
                                  String responseMsg, List<Metadata> metadataList) {
            this.state = state;
            this.status = status;
            this.responseMsg = responseMsg;
            this.metadataList = metadataList;
        }
    }
}
