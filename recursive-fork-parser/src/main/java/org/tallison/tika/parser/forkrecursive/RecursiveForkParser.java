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

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;



public class RecursiveForkParser {

    /** Serial version UID */
    private static final long serialVersionUID = -4962742892274663950L;

    /** Full Java command line */
    private final List<String> java;

    /** Process pool size */
    private int poolSize = 5;

    private int currentlyInUse = 0;

    private final Queue<TikaClient> pool = new LinkedList<>();

    /**
     * Returns the size of the process pool.
     *
     * @return process pool size
     */
    public synchronized int getPoolSize() {
        return poolSize;
    }

    /**
     * Sets the size of the process pool.
     *
     * @param poolSize process pool size
     */
    public synchronized void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }


    /**
     * Returns the command used to start the forked server process.
     * <p/>
     * Returned list is unmodifiable.
     * @return java command line args
     */
    public List<String> getJava() {
        return java;
    }

    public RecursiveForkParser(String[] java) {
        this(Arrays.asList(java));
    }

    /**
     * Initialize with the full commandline to start the child processes
     * e.g. java, -Xmx1g, -cp, classPath
     * @param java
     */
    public RecursiveForkParser(List<String> java) {
        this.java = Collections.unmodifiableList(java);
    }

    public List<Metadata> parse(Path file) throws TikaException {

        List<Metadata> metadataList = null;
        boolean alive = false;
        TikaClient client = acquireClient();
        try {
            metadataList = client.parse(file);
            alive = true;
        } finally {
            releaseClient(client, alive);
        }
        return metadataList;
    }

    public synchronized void close() {
        for (TikaClient client : pool) {
            client.close();
        }
        pool.clear();
        poolSize = 0;
    }

    private synchronized TikaClient acquireClient()
            throws FatalTikaClientException {
        while (true) {
            TikaClient client = pool.poll();
            // Create a new process if there's room in the pool
            if (client == null && currentlyInUse < poolSize) {
                client = new TikaClient(java);
            }

            // Ping the process, and get rid of it if it's inactive
            if (client != null && !client.ping()) {
                    client.close();
                    client = null;
            }

            if (client != null) {
                currentlyInUse++;
                return client;
            } else if (currentlyInUse >= poolSize) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    throw new FatalTikaClientException(
                            "Interrupted while waiting for a fork parser", e);
                }
            }
        }
    }

    private synchronized void releaseClient(TikaClient client, boolean alive) {
        currentlyInUse--;
        if (currentlyInUse + pool.size() < poolSize && alive) {
            pool.offer(client);
        } else {
            client.close();
        }
        notifyAll();
    }

}
