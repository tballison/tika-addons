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

public class TestAutoDetectUnraveler extends UnravelerTestBase {

    @Test
    public void testBasic() throws Exception {
        tearDown();
        for (String docName : new String[]{
                "test-documents.tar",
                "test-documents.tar.Z",
                "test-documents.tbz2",
                "test-documents.tgz",
                "test-documents.zip"
        }) {
            setUp();
            try {
                testEach(docName);
            } finally {
                tearDown();
            }
        }
    }

    private void testEach(String testFileName) throws Exception {

        AutoDetectUnraveler unraveler = new AutoDetectUnraveler(new AutoDetectParser(),
                new BasicContentHandlerFactory(BasicContentHandlerFactory.HANDLER_TYPE.TEXT, -1),
                new DefaultPostParseHandler(tmpDir, null));

        try (InputStream is = getResourceAsStream("/test-documents/"+testFileName)) {
            unraveler.parse(is, new DefaultHandler(), new Metadata(), new ParseContext());
        }

        int sz = tmpDir.resolve("000/000").toFile().listFiles().length;
        assertEquals("number of json files", 9, sz);
        /*
        List<Metadata> metadataList;
        try (Reader reader = Files.newBufferedReader(tmpDir.resolve("000/000/000000000.json"), StandardCharsets.UTF_8)) {
            metadataList = JsonMetadataList.fromJson(reader);
        }
        assertContains("Feuil3", metadataList.get(0).get(RecursiveParserWrapper.TIKA_CONTENT));
        */
    }
}
