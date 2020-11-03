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
import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.HeaderElementIterator;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.util.EntityUtils;
import org.apache.tika.utils.ProcessUtils;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

public class HttpUtils {

    static long HARD_TIMEOUT_MILLIS = 2*60*1000;
    static int MAX_WGET_RETRIES = 2;

    static long MAX_THROTTLE_ATTEMPTS = 4;
    static long THROTTLE_SLEEP_INCREMENTS_MILLIS = 120000;

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
                    System.err.println("error: "+msg);
                    for (Header header : httpResponse.getAllHeaders()) {
                        System.err.println(header);
                    }
                    throw new ClientException("Bad status code: " +
                            httpResponse.getStatusLine().getStatusCode()
                            + "for url: " + url);
                }

                return EntityUtils.toByteArray(httpResponse.getEntity());
            }
        } catch (IOException e) {
            throw new ClientException(url, e);
        }
    }

    /**
     *
     *
     * @param url url-encoded url -- this does not encode the url!
     * @return
     * @throws ClientException
     */
    public static void get(String url, Path targetPath) throws ClientException {
        get(getClient(url), url, targetPath);
    }

    public static void get(HttpClient httpClient, String url, Path targetPath) throws ClientException {

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

        final HttpGet finalHttpGet = httpGet;
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                if (finalHttpGet != null) {
                    finalHttpGet.abort();
                }
            }
        };
        new Timer(true).schedule(task, HARD_TIMEOUT_MILLIS);
        HttpResponse httpResponse = null;
        try {
            httpResponse = httpClient.execute(target, httpGet);
                if (httpResponse.getStatusLine().getStatusCode() != 200) {
                    String msg = new String(EntityUtils.toByteArray(
                            httpResponse.getEntity()), StandardCharsets.UTF_8);
                    throw new ClientException("Bad status code: " +
                            httpResponse.getStatusLine().getStatusCode()
                            + " for url: " + url);
                }

                Files.copy(httpResponse.getEntity().getContent(), targetPath,
                        StandardCopyOption.REPLACE_EXISTING);

        } catch (IOException e) {
            throw new ClientException(url, e);
        } finally {
            if (httpResponse != null && httpResponse instanceof CloseableHttpClient) {
                try {
                    ((CloseableHttpResponse)httpResponse).close();
                } catch (IOException e) {
                    //silently swallow
                }
            }
        }
    }

    public static byte[] get(HttpClient httpClient, String url) throws IOException {
        //overly simplistic...need to add proxy, etc., but good enough for now
        URI uri = null;
        try {
            uri = new URI(url);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }

        HttpHost target = new HttpHost(uri.getHost(), uri.getPort(), uri.getScheme());
        HttpGet httpGet = null;
        try {
            String get = uri.getPath();
            if (!StringUtils.isBlank(uri.getQuery())) {
                get += "?" + uri.getRawQuery();
            }
            httpGet = new HttpGet(get);
        } catch (Exception e) {
            throw new IllegalArgumentException(url, e);
        }

        HttpResponse httpResponse = null;
        try {

            httpResponse = httpClient.execute(target, httpGet);
            int throttleAttempts = 0;
            while (httpResponse.getStatusLine().getStatusCode() == 429 && throttleAttempts < MAX_THROTTLE_ATTEMPTS) {
                long throttleAmount = (++throttleAttempts) * THROTTLE_SLEEP_INCREMENTS_MILLIS;
                System.err.println("received 429; must be throttling; sleeping for "+throttleAmount+" ms.");
                try {
                    Thread.sleep(throttleAmount);
                } catch (InterruptedException e) {

                }
                httpResponse = httpClient.execute(target, httpGet);
            }
            if (httpResponse.getStatusLine().getStatusCode() != 200) {
                String msg = new String(EntityUtils.toByteArray(
                        httpResponse.getEntity()), StandardCharsets.UTF_8);
                throw new IOException("Bad status code: " +
                        httpResponse.getStatusLine().getStatusCode()
                        + "for url: " + url + "; msg: " + msg);
            }
            return EntityUtils.toByteArray(httpResponse.getEntity());

        } finally {
            if (httpResponse != null && httpResponse instanceof CloseableHttpResponse) {
                    ((CloseableHttpResponse) httpResponse).close();
            }
        }
    }

    public static HttpClient getClient(String url,
                                       String username, String password)
            throws ClientException {

        String scheme = null;
        try {
            scheme = new URI(url).getScheme();
        } catch (URISyntaxException e) {
            throw new ClientException(e);
        }
        if (scheme.endsWith("s")) {
            try {
                return httpClientTrustingAllSSLCerts2(username, password);
            } catch (NoSuchAlgorithmException|KeyManagementException|KeyStoreException e) {
                throw new ClientException(e);
            }
        } else if (username != null && password != null) {
            CredentialsProvider provider = getProvider(username, password);
            return HttpClientBuilder.create()
                    .setKeepAliveStrategy(getDefaultKeepAliveStrategy())
                    .setDefaultCredentialsProvider(provider)
                    .build();
        } else {
            return HttpClientBuilder.create()
                    .setKeepAliveStrategy(getDefaultKeepAliveStrategy())
                    .build();
        }
    }


    public static HttpClient getClient(String authority) throws ClientException {
        return getClient(authority, null, null);
    }

    public static void wget(String url, Path outputFile) throws InterruptedException, ClientException, IOException {

        String[] args = new String[]{ "wget", url,
                "--timeout="+(long)((double)HARD_TIMEOUT_MILLIS/(double)1000),//timeout in seconds
                "--tries="+MAX_WGET_RETRIES,
                "-O", ProcessUtils.escapeCommandLine(outputFile.toAbsolutePath().toString())};

        ProcessBuilder builder = new ProcessBuilder(args);
        builder.inheritIO();
        Process p = builder.start();
        p.waitFor(HARD_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        try {
            int exitValue = p.exitValue();
            if (exitValue != 0) {
                throw new ClientException("bad exit value: "+exitValue);
            }
        } catch (IllegalThreadStateException e) {
            System.err.println("timeout on : "+url + " -> "+ outputFile);
            p.destroyForcibly();
            throw new ClientException("timeout");
        }
    }

    private static HttpClient httpClientTrustingAllSSLCerts2(String username,
                                                             String password)
            throws NoSuchAlgorithmException, KeyManagementException, KeyStoreException {
        CredentialsProvider provider = getProvider(username, password);
        TrustStrategy acceptingTrustStrategy = (cert, authType) -> true;
        SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(null,
                acceptingTrustStrategy).build();
        SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext,
                NoopHostnameVerifier.INSTANCE);

        Registry<ConnectionSocketFactory> socketFactoryRegistry =
                RegistryBuilder.<ConnectionSocketFactory>create()
                        .register("https", sslsf)
                        .register("http", new PlainConnectionSocketFactory())
                        .build();

        BasicHttpClientConnectionManager connectionManager =
                new BasicHttpClientConnectionManager(socketFactoryRegistry);
        if (provider == null) {
            return HttpClients.custom()
                    .setKeepAliveStrategy(getDefaultKeepAliveStrategy())
                    .setSSLSocketFactory(sslsf)
                    .setConnectionManager(connectionManager)
                    .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE).build();

        } else {
            return HttpClients.custom()
                    .setKeepAliveStrategy(getDefaultKeepAliveStrategy())
                    .setSSLSocketFactory(sslsf)
                    .setConnectionManager(connectionManager)
                    .setDefaultCredentialsProvider(provider)
                    .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE).build();
        }
    }

    /**
     * can return null if username and password are both null
     *
     * @param username
     * @param password
     * @return
     */
    private static CredentialsProvider getProvider(String username, String password) {
        if ((username == null && password != null) ||
                (password == null && username != null)) {
            throw new IllegalArgumentException("can't have one of 'username', " +
                    "'password' null and the other not");
        }
        if (username != null && password != null) {
            CredentialsProvider provider = new BasicCredentialsProvider();
            UsernamePasswordCredentials credentials
                    = new UsernamePasswordCredentials(username, password);
            provider.setCredentials(AuthScope.ANY, credentials);
            return provider;
        }
        return null;
    }

    //if no keep-alive header is found or a bad value is present,
    // keep alive for only 5 seconds!
    private static ConnectionKeepAliveStrategy getDefaultKeepAliveStrategy() {
        return new ConnectionKeepAliveStrategy() {

            public long getKeepAliveDuration(HttpResponse response, HttpContext context) {
                // Honor 'keep-alive' header
                HeaderElementIterator it = new BasicHeaderElementIterator(
                        response.headerIterator(HTTP.CONN_KEEP_ALIVE));
                while (it.hasNext()) {
                    HeaderElement he = it.nextElement();
                    String param = he.getName();
                    String value = he.getValue();
                    if (value != null && param != null &&
                            param.equalsIgnoreCase("timeout")) {
                        try {
                            return Long.parseLong(value) * 1000;
                        } catch (NumberFormatException ignore) {
                        }
                    }
                }
                return 5 * 1000;
            }

        };
    }
}
