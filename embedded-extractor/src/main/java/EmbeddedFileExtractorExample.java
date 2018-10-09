import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.poi.poifs.filesystem.DirectoryEntry;
import org.apache.poi.poifs.filesystem.DocumentEntry;
import org.apache.poi.poifs.filesystem.DocumentInputStream;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.mime.MimeTypeException;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This is a simple example of code to extract emfs from
 * a list of files.
 */
public class EmbeddedFileExtractorExample {

    Parser parser = new AutoDetectParser();
    Detector detector = TikaConfig.getDefaultConfig().getDetector();

    public static void main(String[] args) throws Exception {
        EmbeddedFileExtractorExample embeddedFileExtractorExample = new EmbeddedFileExtractorExample();
        Path docRoot = Paths.get(args[0]);
        BufferedReader reader = Files.newBufferedReader(Paths.get(args[1]), StandardCharsets.UTF_8);
        Path targRoot = Paths.get(args[2]);
        embeddedFileExtractorExample.execute(docRoot, reader, targRoot);
    }

    private void execute(Path docRoot, BufferedReader reader, Path targRoot) throws IOException {
        String line = reader.readLine();


        while (line != null) {
            String[] parts = line.split("\t");
            extract(docRoot, parts[0], targRoot);
            line = reader.readLine();
        }
    }

    private void extract(Path docRoot, String part, Path targRoot) {
        Path file = docRoot.resolve(part);
        Metadata metadata = new Metadata();
        try (TikaInputStream tis = TikaInputStream.get(file, metadata)) {
            ParseContext parseContext = new ParseContext();
            parseContext.set(EmbeddedDocumentExtractor.class,
                    new FileEmbeddedDocumentExtractor(docRoot, file, targRoot));
            parser.parse(tis, new DefaultHandler(), metadata, parseContext);
        } catch (Throwable t) {
            System.err.println(file + " : " + t);
        }
    }

    private class FileEmbeddedDocumentExtractor
            implements EmbeddedDocumentExtractor {

        private final Path docRoot;
        private final Path doc;
        private final Path targRoot;
        private int count = 0;
        private final TikaConfig config = TikaConfig.getDefaultConfig();

        private FileEmbeddedDocumentExtractor(Path docRoot, Path doc, Path targRoot) {
            this.docRoot = docRoot;
            this.doc = doc;
            this.targRoot = targRoot;
        }

        public boolean shouldParseEmbedded(Metadata metadata) {
            return true;
        }

        public void parseEmbedded(InputStream inputStream, ContentHandler contentHandler, Metadata metadata, boolean outputHtml) throws SAXException, IOException {

            if (! inputStream.markSupported()) {
                inputStream = TikaInputStream.get(inputStream);
            }
            MediaType contentType = detector.detect(inputStream, metadata);
            if (! contentType.toString().contains("image/emf")) {
                return;
            }
            Path outputFile = targRoot.resolve(docRoot.relativize(doc)+"_"+count++ +".emf");


            Path parent = outputFile.getParent();
            if (!Files.isDirectory(parent)) {
                Files.createDirectories(parent);
            }
            System.out.println("Extracting '"+doc+"' ("+contentType+") to " + outputFile);

            try (OutputStream os = Files.newOutputStream(outputFile)) {
                if (inputStream instanceof TikaInputStream) {
                    TikaInputStream tin = (TikaInputStream) inputStream;

                    if (tin.getOpenContainer() != null && tin.getOpenContainer() instanceof DirectoryEntry) {
                        POIFSFileSystem fs = new POIFSFileSystem();
                        copy((DirectoryEntry) tin.getOpenContainer(), fs.getRoot());
                        fs.writeFilesystem(os);
                    } else {
                        IOUtils.copy(inputStream, os);
                    }
                } else {
                    IOUtils.copy(inputStream, os);
                }
            } catch (Exception e) {
                //
                // being a CLI program messages should go to the stderr too
                //
                String msg = String.format(
                        Locale.ROOT,
                        "Ignoring unexpected exception trying to save embedded file %s (%s)",
                        doc,
                        e.getMessage()
                );
            }
        }

        private Path getOutputFile(String name, Metadata metadata, MediaType contentType) {
            String ext = getExtension(contentType);
            if (name.indexOf('.')==-1 && contentType!=null) {
                name += ext;
            }

            //defensively do this so that we don't get an exception
            //from FilenameUtils.normalize
            name = name.replaceAll("\u0000", " ");
            String normalizedName = FilenameUtils.normalize(name);

            if (normalizedName == null) {
                normalizedName = FilenameUtils.getName(name);
            }

            if (normalizedName == null) {
                normalizedName = "file"+count++ +ext;
            }
            //strip off initial C:/ or ~/ or /
            int prefixLength = FilenameUtils.getPrefixLength(normalizedName);
            if (prefixLength > -1) {
                normalizedName = normalizedName.substring(prefixLength);
            }

            Matcher govdocsMatcher = Pattern.compile("^\\d{6}\\.").matcher(name);
            Matcher commonCrawl = Pattern.compile("[A-Z0-9]{32}").matcher(name);
            if (govdocsMatcher.find()) {
                name = "govdocs1/"+name.substring(0,3)+"/"+name;
            } else if (commonCrawl.find()){
                name = "commoncrawl2/"+name.substring(0,2)+"/"+name;
            }

            Path outputFile = targRoot.resolve(name);
            //if file already exists, prepend uuid
            if (Files.exists(outputFile)) {
                String fileName = FilenameUtils.getName(normalizedName);
                outputFile = targRoot.resolve(fileName+"-"+UUID.randomUUID().toString());
            }
            return outputFile;
        }

        private String getExtension(MediaType contentType) {
            try {
                String ext = config.getMimeRepository().forName(
                        contentType.toString()).getExtension();
                if (ext == null) {
                    return ".bin";
                } else {
                    return ext;
                }
            } catch (MimeTypeException e) {
                e.printStackTrace();
            }
            return ".bin";

        }

        protected void copy(DirectoryEntry sourceDir, DirectoryEntry destDir)
                throws IOException {
            for (org.apache.poi.poifs.filesystem.Entry entry : sourceDir) {
                if (entry instanceof DirectoryEntry) {
                    // Need to recurse
                    DirectoryEntry newDir = destDir.createDirectory(entry.getName());
                    copy((DirectoryEntry) entry, newDir);
                } else {
                    // Copy entry
                    try (InputStream contents =
                                 new DocumentInputStream((DocumentEntry) entry)) {
                        destDir.createDocument(entry.getName(), contents);
                    }
                }
            }
        }
    }

}
