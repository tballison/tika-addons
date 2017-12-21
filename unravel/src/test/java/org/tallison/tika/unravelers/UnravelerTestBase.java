package org.tallison.tika.unravelers;

import org.apache.commons.io.FileUtils;
import org.apache.tika.TikaTest;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class UnravelerTestBase extends TikaTest {

    protected Path tmpDir = null;
    @Before
    public void setUp() throws IOException {
        tmpDir = Files.createTempDirectory("tst-unravel");
    }

    @After
    public void tearDown() throws IOException {
        FileUtils.deleteDirectory(tmpDir.toFile());
    }
}
