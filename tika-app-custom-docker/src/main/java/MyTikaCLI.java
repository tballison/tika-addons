import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.xml.sax.ContentHandler;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.extractor.ParsingEmbeddedDocumentExtractor;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.ToXMLContentHandler;

public class MyTikaCLI {

    public static void main(String[] args) throws Exception {
        Path tikaConfigPath = Paths.get(args[0]);
        Path inputFile = Paths.get(args[1]);
        Path outputFile = Paths.get(args[2]);
        AutoDetectParser parser = new AutoDetectParser(new TikaConfig(tikaConfigPath));
        try (InputStream is = TikaInputStream.get(inputFile);
             OutputStream os = Files.newOutputStream(outputFile)) {//, StandardCharsets.UTF_8)) {
            Metadata metadata = new Metadata();
            ContentHandler handler = new ToXMLContentHandler(os, "UTF-8");
            ParseContext pc = new ParseContext();
            EmbeddedDocumentExtractor ex = new ParsingEmbeddedDocumentExtractor(pc);
            pc.set(EmbeddedDocumentExtractor.class, ex);
            parser.parse(is, handler, metadata, pc);
        }
    }
}
