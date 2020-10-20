import org.apache.tika.TikaTest;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.junit.BeforeClass;
import org.junit.Test;
import org.tallison.tika.metadata.ParseStatus;
import org.xml.sax.ContentHandler;

import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestMultithreaded {

    private static Path TEST_FILE;

    @BeforeClass
    public static void setUp() throws URISyntaxException {
        TEST_FILE = Paths.get(
                TestMultithreaded.class.getResource("testPDF_corrupt.pdf").toURI());
    }

    @Test
    public void testOne() throws Exception {
        Parser p = new AutoDetectParser();
        int numThreads = 50;
        int numIterations = 100;
        ExecutorService ex = Executors.newFixedThreadPool(numThreads);
        ExecutorCompletionService<Integer> completionService = new ExecutorCompletionService<>(ex);

        for (int i = 0; i < numThreads; i++) {
            completionService.submit(new Runner(p, numIterations));
        }
        int completed = 0;
        while (completed++ < numThreads) {
            Future<Integer> future = completionService.take();
            future.get();
        }
    }

    private static class Runner implements Callable<Integer> {
        private final Parser parser;
        int iterations;
        private Runner(Parser p, int iterations) {
            this.parser = p;
            this.iterations = iterations;
        }
        @Override
        public Integer call()  {

            for (int i = 0; i < iterations; i++) {
                Metadata m = new Metadata();
                ParseContext pc = new ParseContext();
                ContentHandler contentHandler = new BodyContentHandler();
                try (InputStream is = TikaInputStream.get(TEST_FILE)) {
                    parser.parse(is, contentHandler, m, pc);
                } catch (TikaException e) {
                    //e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
                assertEquals("unsafe", m.get(ParseStatus.SAFETY_STATUS));
                assertEquals("rejected", m.get(ParseStatus.VALIDITY_STATUS));

                String[] warnings = m.getValues(ParseStatus.WARNINGS);
                assertTrue(warnings[warnings.length - 1].contains("Leftover args"));
            }
            return 1;
        }
    }
}
