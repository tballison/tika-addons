import org.apache.tika.TikaTest;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.pkg.PackageParser;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;

public class UnpackerTest extends TikaTest {
    @Test
    public void testOneOff() throws Exception {
        getXML(
                UnpackerTest.class.getResourceAsStream("/zip64-sample.zip"),
                new AutoDetectParser(), new Metadata());
    }
}
