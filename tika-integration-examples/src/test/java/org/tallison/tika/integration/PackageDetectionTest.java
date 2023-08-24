package org.tallison.tika.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.InputStream;


import org.junit.jupiter.api.Test;

import org.apache.tika.Tika;
import org.apache.tika.detect.Detector;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;

public class PackageDetectionTest {

    @Test
    public void testOOXMLDetection() throws Exception {
        Tika tika = new Tika();
        String fileName = "Testworddocx.docx";
        testStream(tika.getDetector(), fileName, "application/vnd.openxmlformats-officedocument.wordprocessingml.document");
        testPath(tika.getDetector(), fileName, "application/vnd.openxmlformats-officedocument.wordprocessingml.document");

        fileName = "Testworddocx2.docx";
        testStream(tika.getDetector(), fileName, "application/x-tika-ooxml");
        testPath(tika.getDetector(), fileName, "application/x-tika-ooxml");
    }

    private void testPath(Detector detector, String fileName, String expectedMime) throws Exception {
        try (TikaInputStream tis = TikaInputStream.get(
                getClass().getResourceAsStream("/test-documents/" + fileName))) {
            try {
                tis.getPath();
                String mimeType = detector.detect(tis, new Metadata()).toString();
                assertEquals(expectedMime, mimeType);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    private void testStream(Detector detector, String fileName, String expectedMime) throws Exception {
        try (InputStream stream = getClass().getResourceAsStream("/test-documents/" + fileName)) {
            try {
                String mimeType = detector.detect(stream, new Metadata()).toString();
                assertEquals(expectedMime, mimeType);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }
}

