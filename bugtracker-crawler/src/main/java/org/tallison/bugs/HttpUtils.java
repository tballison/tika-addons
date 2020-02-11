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

import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;

class HttpUtils {

    /**
     *
     * @param url url-encoded url -- this does not encode the url!
     * @return
     * @throws ClientException
     */
    public static byte[] get(String url) throws ClientException {
        //overly simplistic...need to add proxy, etc., but good enough for now
        URI uri = null;
        try {
            uri = new URI(url);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
        HttpHost target = new HttpHost(uri.getHost(), uri.getPort());
        HttpGet httpGet = null;
        try {
            String get = uri.getRawPath();
            if (!StringUtils.isBlank(uri.getRawQuery())) {
                get += "?" + uri.getRawQuery();
            }
            httpGet = new HttpGet(get);
        } catch (Exception e) {
            throw new IllegalArgumentException(url, e);
        }

        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            try (CloseableHttpResponse httpResponse = httpClient.execute(target, httpGet)) {
                if (httpResponse.getStatusLine().getStatusCode() != 200) {
                    String msg = new String(EntityUtils.toByteArray(
                            httpResponse.getEntity()), StandardCharsets.UTF_8);
                    throw new ClientException("Bad status code: " +
                            httpResponse.getStatusLine().getStatusCode()
                            + "for url: " + url + "; msg: " + msg);
                }
                return EntityUtils.toByteArray(httpResponse.getEntity());
            }
        }
        catch (IOException e) {
            throw new ClientException(url, e);
        }
    }
}
