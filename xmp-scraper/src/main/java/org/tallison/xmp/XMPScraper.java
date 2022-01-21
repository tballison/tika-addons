/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tallison.xmp;

import static java.nio.charset.StandardCharsets.US_ASCII;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.util.Iterator;

public class XMPScraper implements Iterable<XMPResult> {
    private static final byte[] PACKET_HEADER_START;
    private static final byte[] PACKET_HEADER_TRAILER_END;
    private static final byte[] PACKET_TRAILER_START;

    static {
        PACKET_HEADER_START = "<?xpacket begin=".getBytes(US_ASCII);
        PACKET_HEADER_TRAILER_END = "?>".getBytes(US_ASCII);
        PACKET_TRAILER_START = "<?xpacket".getBytes(US_ASCII);
    }

    private final InputStream is;

    private int maxCache = 100_000_000;

    public XMPScraper(InputStream is) {
        this.is = is;
    }

    /**
     * This throws an XMPScannerException that wraps IOExceptions
     * and other parse problems including EOF
     *
     * @return
     */
    @Override
    public Iterator<XMPResult> iterator() {
        return new XMPIterator();
    }

    private class XMPIterator implements Iterator<XMPResult> {

        StreamSearcher headerStart = new StreamSearcher(PACKET_HEADER_START);
        StreamSearcher headerTrailerEnd;
        StreamSearcher trailerStart;

        XMPResult next = null;
        long totalRead = 0;

        private XMPIterator() {
            try {
                _next();
            } catch (IOException e) {
                throw new XMPScannerException(e);
            }
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public XMPResult next() {
            XMPResult ret = next;
            try {
                _next();
            } catch (IOException|IllegalArgumentException e) {
                throw new XMPScannerException(e);
            }
            return ret;
        }

        private void _next() throws IOException {
            next = null;

            //find the packet header start
            long read = headerStart.search(is);
            if (read < 0) {
                return;
            }
            totalRead += read;
            long offset = totalRead - PACKET_HEADER_START.length;
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            bos.write(PACKET_HEADER_START);

            if (headerTrailerEnd == null) {
                headerTrailerEnd = new StreamSearcher(PACKET_HEADER_TRAILER_END);
            }
            read = headerTrailerEnd.search(is, bos, maxCache);
            if (read < 0) {
                throw new XMPScannerException(new EOFException());
            }

            totalRead += read;
            byte[] header = bos.toByteArray();
            //read the payload up to and including the start of the packet trailer
            bos.reset();

            if (trailerStart == null) {
                trailerStart = new StreamSearcher(PACKET_TRAILER_START);
            }
            read = trailerStart.search(is, bos, maxCache);
            if (read < 0) {
                throw new XMPScannerException(new EOFException());
            }
            totalRead += read;

            //trim the packet trailer start from the bytes
            byte[] bytes = bos.toByteArray();
            byte[] payload = new byte[bytes.length - PACKET_TRAILER_START.length];
            System.arraycopy(bytes, 0, payload, 0, bytes.length - PACKET_TRAILER_START.length);
            bytes = null;
            bos.reset();
            //find the end of the trailer
            bos.write(PACKET_TRAILER_START);
            read = headerTrailerEnd.search(is, bos, maxCache);
            if (read < 0) {
                throw new XMPScannerException(new EOFException());
            }
            totalRead += read;
            byte[] trailer = bos.toByteArray();
            bos.reset();
            next = new XMPResult(offset, header, payload, trailer);

        }

    }
}
