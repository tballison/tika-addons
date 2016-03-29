package org.tallison.parser.pdf;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;

import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.pdf.PDFParser;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

public class ComboPDFParser extends AbstractParser {
    private static final MediaType MEDIA_TYPE = MediaType.application("pdf");

    private static final Set<MediaType> SUPPORTED_TYPES =
            Collections.singleton(MEDIA_TYPE);

    public Set<MediaType> getSupportedTypes(ParseContext context) {
        return SUPPORTED_TYPES;
    }

    public void parse(InputStream inputStream, ContentHandler contentHandler, Metadata metadata, ParseContext parseContext) throws IOException, SAXException, TikaException {

        TemporaryResources temporaryResources = new TemporaryResources();
        Path tmpFile = temporaryResources.createTempFile();
        Files.copy(inputStream, tmpFile, REPLACE_EXISTING);
        try {
            parse(tmpFile, contentHandler, metadata, parseContext);
        } finally {
            Files.delete(tmpFile);
        }
    }

    public void parse(Path path, ContentHandler contentHandler, Metadata metadata, ParseContext parseContext) throws IOException, SAXException, TikaException {
        boolean ex = false;
        PDFParser p = new PDFParser();
        try (InputStream is = TikaInputStream.get(path)){
            p.parse(is, contentHandler, metadata, parseContext);
        } catch (IOException|TikaException e) {
            ex = true;
        }
        if (ex) {
            org.apache.tika.parser.pdf18.PDFParser p18 = new org.apache.tika.parser.pdf18.PDFParser();
            metadata.add("X-Parsed-By", p18.getClass().getName());
            try (InputStream is = TikaInputStream.get(path)) {
                p18.parse(is, contentHandler, metadata, parseContext);
            }
        } else {
            metadata.add("X-Parsed-By", p.getClass().getName());
        }
    }
}
