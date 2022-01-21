package org.tallison.xmp;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.Test;

public class TestXMPSpelunker {

    @Test
    public void testBasic() throws Exception {
        XMPSpelunker.processFile(getFile("OOO-124375-1.zip-1.pdf"));
    }

    private File getFile(String s) throws Exception {
        return Paths.get(
                TestXMPSpelunker.class.getResource("/"+s).toURI()).toFile();
    }
}
