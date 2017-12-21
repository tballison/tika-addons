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

package org.tallison.tika.unravelers;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.serialization.JsonMetadataList;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.RecursiveParserWrapper;
import org.apache.tika.sax.BasicContentHandlerFactory;
import org.junit.Test;
import org.xml.sax.helpers.DefaultHandler;

import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestPSTUnraveler extends UnravelerTestBase {


    @Test
    public void testRecursive() throws Exception {
        AutoDetectUnraveler pstUnraveler = new AutoDetectUnraveler(new AutoDetectParser(),
                new BasicContentHandlerFactory(BasicContentHandlerFactory.HANDLER_TYPE.TEXT, -1),
                new DefaultPostParseHandler(tmpDir, null));
        try (InputStream is = getResourceAsStream("/test-documents/test_embedded.pst")) {
            pstUnraveler.parse(is, new DefaultHandler(), new Metadata(), new ParseContext());
        }


        List<Metadata> metadataList;
        try (Reader reader = Files.newBufferedReader(tmpDir.resolve("000/000/000000001.json"), StandardCharsets.UTF_8)) {
            metadataList = JsonMetadataList.fromJson(reader);
        }
        assertEquals("/test_recursive_embedded.docx/embed1.zip/embed2.zip/embed3.zip/embed4.zip", metadataList.get(9).get(RecursiveParserWrapper.EMBEDDED_RESOURCE_PATH));
    }
}

