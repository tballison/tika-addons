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

import static org.apache.commons.io.ByteOrderMark.UTF_16BE;
import static org.apache.commons.io.ByteOrderMark.UTF_16LE;
import static org.apache.commons.io.ByteOrderMark.UTF_8;
import static java.nio.charset.StandardCharsets.US_ASCII;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class XMPResult {

    private static final byte[] BEGIN_ATTR_START = "begin=".getBytes(US_ASCII);
    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
    private static final byte SINGLE_QUOTE = (byte) '\'';
    private static final byte DOUBLE_QUOTE = (byte) '"';

    private final long startOffset;
    private Charset charset = null;
    private final byte[] header;
    private final byte[] payload;
    private final byte[] trailer;

    public XMPResult(long startOffset, byte[] header, byte[] payload,
                     byte[] trailer) {
        this.startOffset = startOffset;
        this.header = header;
        this.payload = payload;
        this.trailer = trailer;
    }

    public byte[] getHeader() {
        return header;
    }

    public byte[] getPayload() {
        return payload;
    }

    public byte[] getTrailer() {
        return trailer;
    }

    /**
     * If the encoding is set in the begin= attribute in the packet header,
     * this returns the encoding if recognized.  If non-existent or not
     * recognized, this returns null;
     *
     * @return
     */
    public Charset getPayloadEncoding() {
        if (charset != null) {
            return charset;
        }
        Charset cs = null;
        try {
            cs = scrapeCharsetFromBeginAttr(header);
        } catch (IOException e) {
            //swallow
        }
        this.charset = cs;
        return this.charset;
    }

    private Charset scrapeCharsetFromBeginAttr(byte[] header) throws IOException {
        StreamSearcher beginAttr = new StreamSearcher(BEGIN_ATTR_START);

        InputStream is = new ByteArrayInputStream(header);
        long start = beginAttr.search(is);
        if (start < 0) {
            return DEFAULT_CHARSET;
        }
        ByteArrayOutputStream encodingBytes = new ByteArrayOutputStream();
        int c = is.read();
        if (c == -1) {
            return DEFAULT_CHARSET;
        }
        boolean success = false;
        if (SINGLE_QUOTE == (byte) c || DOUBLE_QUOTE == (byte) c) {
            c = is.read();
            while (c > -1) {
                if (SINGLE_QUOTE == (byte) c || DOUBLE_QUOTE == (byte) c) {
                    success = true;
                    break;
                } else {
                    encodingBytes.write(c);
                }
                c = is.read();
            }
        }
        if (!success) {
            return DEFAULT_CHARSET;
        }
        byte[] encodingByteArr = encodingBytes.toByteArray();
        if (Arrays.equals(encodingByteArr, UTF_8.getBytes())) {
            return StandardCharsets.UTF_8;
        } else if (Arrays.equals(encodingByteArr, UTF_16BE.getBytes())) {
            return StandardCharsets.UTF_16BE;
        } else if (Arrays.equals(encodingByteArr, UTF_16LE.getBytes())) {
            return StandardCharsets.UTF_16LE;
        }
        return null;

    }


    @Override
    public String toString() {
        return "XMPResult{" +
                "startOffset=" + startOffset +
                ", header=" + new String(header, StandardCharsets.US_ASCII) +
                ", payload=" + new String(payload, StandardCharsets.US_ASCII) +
                ", trailer=" + new String(trailer, StandardCharsets.US_ASCII) +
                '}';
    }
}
