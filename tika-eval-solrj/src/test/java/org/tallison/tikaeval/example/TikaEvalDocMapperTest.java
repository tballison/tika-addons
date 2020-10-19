package org.tallison.tikaeval.example;

import org.apache.tika.TikaTest;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.sax.RecursiveParserWrapperHandler;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TikaEvalDocMapperTest extends TikaTest {

    @Test
    public void testBasic() throws Exception {

        //need to have at least 2 of each token so that they aren't dropped from the
        //text profile signature

        //need to have > 3 "the" so that the count is quantized > 2
        String content1 = "the the the quick brown fox JUMPED jumped    over over ---- the quick brown fox";

        String content2 = "the the the quick brown fox jumped jumped over over the quick brown fox";
        String content3 = "the the the the the the the " + content2 + " "+content2;

        Metadata metadata1 = new Metadata();
        metadata1.set(RecursiveParserWrapperHandler.TIKA_CONTENT, content1);

        Metadata metadata2 = new Metadata();
        metadata2.set(RecursiveParserWrapperHandler.TIKA_CONTENT, content2);

        Metadata metadata3 = new Metadata();
        metadata3.set(RecursiveParserWrapperHandler.TIKA_CONTENT, content3);

        TikaEvalDocMapper mapper = new TikaEvalDocMapper();
        List<Metadata> metadataList = new ArrayList<>();
        metadataList.add(metadata1);
        metadataList.add(metadata2);
        metadataList.add(metadata3);

        List<Metadata> docs = mapper.map(metadataList);
        assertEquals(3, docs.size());

        assertEquals(docs.get(0).get("text_digest"), docs.get(1).get("text_digest"));
        assertNotEquals(docs.get(1).get("text_digest"), docs.get(2).get("text_digest"));


        assertEquals(docs.get(0).get("text_profile_digest"), docs.get(1).get("text_profile_digest"));
        assertEquals(docs.get(0).get("text_profile_digest"), docs.get(2).get("text_profile_digest"));

        assertEquals("2", docs.get(0).get("num_attachments"));
        assertEquals("2", docs.get(0).get("total_embedded"));

    }
}
