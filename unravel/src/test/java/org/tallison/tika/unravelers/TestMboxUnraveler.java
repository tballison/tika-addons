package org.tallison.tika.unravelers;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.serialization.JsonMetadataList;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.BasicContentHandlerFactory;
import org.junit.Ignore;
import org.junit.Test;
import org.tallison.tika.unravelers.mbox.MBoxUnraveler;
import org.xml.sax.helpers.DefaultHandler;

import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

public class TestMboxUnraveler extends UnravelerTestBase {
    @Test
    @Ignore("until this is an actual test")
    public void testRecursive() throws Exception {

        MBoxUnraveler unraveler = new MBoxUnraveler(new AutoDetectParser(),
                new BasicContentHandlerFactory(BasicContentHandlerFactory.HANDLER_TYPE.TEXT, -1),
                new DefaultPostParseHandler(tmpDir, null));

        try (InputStream is = getResourceAsStream("/test-documents/testMBOX_complex.mbox")) {
            unraveler.parse(is, new DefaultHandler(), new Metadata(), new ParseContext());
        }

        List<Metadata> metadataList;
        try (Reader reader = Files.newBufferedReader(tmpDir.resolve("000/000/000000000.json"), StandardCharsets.UTF_8)) {
            metadataList = JsonMetadataList.fromJson(reader);
        }
        debug(metadataList);
//        assertEquals("/test_recursive_embedded.docx/embed1.zip/embed2.zip/embed3.zip/embed4.zip", metadataList.get(9).get(RecursiveParserWrapper.EMBEDDED_RESOURCE_PATH));

    }
}
