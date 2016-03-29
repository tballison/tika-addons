package org.tallison.parser.pdf;

import java.util.Arrays;

import org.apache.tika.TikaTest;
import org.apache.tika.metadata.Metadata;
import org.junit.Test;

public class PDFComboParserTest extends TikaTest {

    @Test
    public void testGoodDoc() throws Exception {
        XMLResult r = getXML("testPDF.pdf");
        assertContains("org.apache.tika.parser.pdf.PDFParser", Arrays.asList(r.metadata.getValues("X-Parsed-By")));
        assertContains("Apache Tika project", r.xml);
    }

    @Test
    public void testComboOnBadDoc() throws Exception {

        XMLResult r = getXML("ZZJCCTCSMAOQ6DR57LMRDPDYC56JXMRC.pdf");
        Metadata m = r.metadata;
        assertContains("Syllabus", r.xml);
        assertContains("org.apache.tika.parser.pdf18.PDFParser", Arrays.asList(m.getValues("X-Parsed-By")));
    }
}
