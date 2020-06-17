package org.tallison.tika.client;

import org.apache.tika.TikaTest;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.sax.AbstractRecursiveParserWrapperHandler;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class TikaClientTest extends TikaTest {

    @Test
    public void testBasic() throws Exception {
        String cp = System.getProperty("java.class.path");
        String[] args = new String[] {
                "java", "-Xmx1g", "-cp",
                cp,
                "org.apache.tika.cli.TikaCLI",
                "-J",
                "-t"
        };
        List<Metadata> metadataList = parseOne(args, "ex ample.xml");
        assertEquals("success", metadataList.get(0).get(TikaClient.TIKA_STATUS));
        assertEquals("0", metadataList.get(0).get(TikaClient.EXIT_CODE));
        assertContains("some content", metadataList.get(0).get(
                AbstractRecursiveParserWrapperHandler.TIKA_CONTENT));
    }


    @Test
    public void testBadExitValue() throws Exception {
        String cp = System.getProperty("java.class.path");
        String[] args = new String[] {
                "java", "-Xmx1g", "-cp",
                cp,
                "org.apache.tika.cli.TikaCLI",
                "-J",
                "-t"
        };
        List<Metadata> metadataList = parseOne(args, "system_exit.xml");
        assertEquals("crashed", metadataList.get(0).get(TikaClient.TIKA_STATUS));
        assertEquals("1", metadataList.get(0).get(TikaClient.EXIT_CODE));
    }

    @Test
    public void testStackTrace() throws Exception {
        String cp = System.getProperty("java.class.path");
        String[] args = new String[] {
                "java", "-Xmx1g", "-cp",
                cp,
                "org.apache.tika.cli.TikaCLI",
                "-J",
                "-t"
        };
        List<Metadata> metadataList = parseOne(args, "null_pointer.xml");
        String stacktrace = metadataList.get(0).get(TikaClient.STACK_TRACE);
        assertNotNull(stacktrace);
        assertEquals("success_exception", metadataList.get(0).get(TikaClient.TIKA_STATUS));
        assertEquals("1", metadataList.get(0).get(TikaClient.EXIT_CODE));
        assertTrue(stacktrace.startsWith("Exception in thread"));
        assertContains("NullPointerException", stacktrace);
    }

    @Test
    public void testTimeout() throws Exception {
        String cp = System.getProperty("java.class.path");
        String[] args = new String[] {
                "java", "-Xmx1g", "-cp",
                cp,
                "org.apache.tika.cli.TikaCLI",
                "-J",
                "-t"
        };
        List<Metadata> metadataList = parseOne(args, "heavy_hang.xml", 10000);
        assertEquals("timeout", metadataList.get(0).get(TikaClient.TIKA_STATUS));
        assertEquals("-1", metadataList.get(0).get(TikaClient.EXIT_CODE));
    }

    private List<Metadata> parseOne(String[] args, String file) throws Exception {
        return parseOne(args, file, -1l);
    }

    private List<Metadata> parseOne(String[] args, String file, long timeoutMS) throws Exception {
        TikaClient client = new TikaClient();
        if (timeoutMS > 0) {
            client.setTimeoutMS(timeoutMS);
        }
        List<Metadata> metadataList = null;
        try (TikaInputStream tis = TikaInputStream.get(
                this.getClass().getResource("/test-documents/"+file))) {
            metadataList = client.parse(args, tis);
        }
        return  metadataList;
    }
}
