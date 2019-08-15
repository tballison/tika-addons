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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.GzipCompressingEntity;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.IOUtils;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.serialization.JsonMetadataList;

/**
 * Client for tika-server
 */
public class TikaServerClient implements TikaClient {

    private final static String END_POINT = "/rmeta/text";
    private final HttpClient client;
    private final List<String> urls;

    public TikaServerClient() {
        this("http://localhost:9998");
    }

    public TikaServerClient(String ... urls) {
        this.client = HttpClients.createDefault();
        this.urls = new ArrayList<>();
        for (String u : urls) {
            this.urls.add(u + END_POINT);
        }
    }
    public List<Metadata> parse(InputStream is) throws TikaClientException {
        int index = ThreadLocalRandom.current().nextInt(urls.size());
        HttpPut put = new HttpPut(urls.get(index));

        put.setEntity(new InputStreamEntity(is));
        //this is actually quite a bit faster on streams
        //that can fit in memory
        /*
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            IOUtils.copy(is, bos);
        } catch (IOException e) {
            throw new TikaClientException("doh", e);
        }
        put.setEntity(new ByteArrayEntity(bos.toByteArray()));
        */
        HttpResponse response = null;
        try {
            response = client.execute(put);
        } catch (IOException e) {
            throw new TikaClientException("io exception before response", e);
        }
        if (response.getStatusLine().getStatusCode() == 200) {
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(response.getEntity().getContent(),
                    StandardCharsets.UTF_8))) {
                return JsonMetadataList.fromJson(reader);
            } catch (IOException | TikaException e) {
                throw new TikaClientException("problem with entity", e);
            }
        }
        //do better
        throw new TikaClientException("bad status:"
                + response.getStatusLine().getStatusCode());
    }
}
