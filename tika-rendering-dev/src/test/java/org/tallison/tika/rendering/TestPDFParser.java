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
package org.tallison.tika.rendering;

import java.io.InputStream;

import org.junit.Test;

import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.ToTextContentHandler;

public class TestPDFParser {

    @Test
    public void testOne() throws Exception {
        Parser p = new AutoDetectParser();
        String fileName = "/test-documents/000010.pdf";
        ParseContext context = new ParseContext();
        Metadata metadata = new Metadata();
        ToTextContentHandler textContentHandler = new ToTextContentHandler();
        long start = System.currentTimeMillis();
        try (InputStream is =
                     TikaInputStream.get(TestPDFParser.class.getResourceAsStream(fileName))) {
            p.parse(is, textContentHandler, metadata, context);
        }
        long elapsed = System.currentTimeMillis() - start;
        System.out.println(elapsed + " : " + textContentHandler.toString());
    }

}
