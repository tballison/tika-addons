package org.tallison;

import javax.xml.namespace.QName;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.lang3.StringUtils;
import org.apache.poi.xslf.usermodel.XSLFRelation;
import org.apache.poi.xssf.usermodel.XSSFRelation;
import org.apache.poi.xwpf.usermodel.XWPFRelation;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.XmlRootExtractor;
import org.apache.tika.io.CloseShieldInputStream;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.mime.MediaTypeRegistry;
import org.apache.tika.mime.MimeTypes;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.pkg.ZipContainerDetector;
import org.apache.tika.utils.XMLReaderUtils;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class ZipFeatureDumper {
    static final MediaType TIKA_OOXML = MediaType.application("x-tika-ooxml");
    static final MediaType DOCX =
            MediaType.application("vnd.openxmlformats-officedocument.wordprocessingml.document");
    static final MediaType DOCM =
            MediaType.application("vnd.ms-word.document.macroEnabled.12");
    static final MediaType DOTX =
            MediaType.application("vnd.ms-word.document.macroEnabled.12");
    static final MediaType PPTX =
            MediaType.application("vnd.openxmlformats-officedocument.presentationml.presentation");

    static final MediaType PPSM =
            MediaType.application("vnd.ms-powerpoint.slideshow.macroEnabled.12");
    static final MediaType PPSX =
            MediaType.application("vnd.openxmlformats-officedocument.presentationml.slideshow");
    static final MediaType PPTM =
            MediaType.application("vnd.ms-powerpoint.presentation.macroEnabled.12");
    static final MediaType POTM =
            MediaType.application("vnd.ms-powerpoint.template.macroenabled.12");
    static final MediaType POTX =
            MediaType.application("vnd.openxmlformats-officedocument.presentationml.template");
    static final MediaType THMX =
            MediaType.application("vnd.openxmlformats-officedocument");
    static final MediaType XLSB =
            MediaType.application("vnd.ms-excel.sheet.binary.macroenabled.12");
    static final MediaType XLSX =
            MediaType.application("vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    static final MediaType XLSM =
            MediaType.application("vnd.ms-excel.sheet.macroEnabled.12");
    static final MediaType XPS =
            MediaType.application("vnd.ms-xpsdocument");

    static Map<String, MediaType> OOXML_CONTENT_TYPES = new ConcurrentHashMap<>();
    static {
        OOXML_CONTENT_TYPES.put(XWPFRelation.DOCUMENT.getContentType(), DOCX);
        OOXML_CONTENT_TYPES.put(XWPFRelation.MACRO_DOCUMENT.getContentType(), DOCM);
        OOXML_CONTENT_TYPES.put(XWPFRelation.TEMPLATE.getContentType(), DOTX);

        OOXML_CONTENT_TYPES.put(XSSFRelation.WORKBOOK.getContentType(), XLSX);
        OOXML_CONTENT_TYPES.put(XSSFRelation.MACROS_WORKBOOK.getContentType(), XLSM);
        OOXML_CONTENT_TYPES.put(XSSFRelation.XLSB_BINARY_WORKBOOK.getContentType(), XLSB);
        OOXML_CONTENT_TYPES.put(XSLFRelation.MAIN.getContentType(), PPTX);
        OOXML_CONTENT_TYPES.put(XSLFRelation.MACRO.getContentType(), PPSM);
        OOXML_CONTENT_TYPES.put(XSLFRelation.MACRO_TEMPLATE.getContentType(), POTM);
        OOXML_CONTENT_TYPES.put(XSLFRelation.PRESENTATIONML_TEMPLATE.getContentType(), PPTM);
        OOXML_CONTENT_TYPES.put(XSLFRelation.PRESENTATIONML.getContentType(), PPSX);
        OOXML_CONTENT_TYPES.put(XSLFRelation.PRESENTATION_MACRO.getContentType(), PPTM);
        OOXML_CONTENT_TYPES.put(XSLFRelation.PRESENTATIONML_TEMPLATE.getContentType(), POTX);
        OOXML_CONTENT_TYPES.put(XSLFRelation.THEME_MANAGER.getContentType(), THMX);
        OOXML_CONTENT_TYPES.put("application/vnd.ms-package.xps-fixeddocumentsequence+xml", XPS);
    }


    private static final MediaType ZIP = MediaType.application("zip");
    private final MimeTypes mimeTypes = TikaConfig.getDefaultConfig().getMimeRepository();
    private final MediaTypeRegistry mediaTypeRegistry = TikaConfig.getDefaultConfig().getMediaTypeRegistry();
    private final ZipContainerDetector zipContainerDetector = new ZipContainerDetector();


    public static void main(String[] args) throws Exception {
        Path docs = Paths.get(args[0]);
        Writer writer = Files.newBufferedWriter(
                Paths.get(args[1]), StandardCharsets.UTF_8);

        ZipFeatureDumper zipFeatureDumper = new ZipFeatureDumper();
        zipFeatureDumper.execute(docs, writer);
        writer.flush();
        writer.close();
    }

    private void execute(Path docs, Writer writer) {
        processDirectory(docs, writer);
    }

    private void processDirectory(Path docs, Writer writer) {
        for (File f : docs.toFile().listFiles()) {
            if (f.isDirectory()) {
                processDirectory(f.toPath(), writer);
            } else {
                processFile(f, writer);
            }
        }
    }

    private void processFile(File f, Writer writer) {
        MediaType mt = null;
        try (TikaInputStream tis = TikaInputStream.get(f)) {
            mt = mimeTypes.detect(tis, new Metadata());
        } catch (Exception e) {
            //e.printStackTrace();
        }
        if (mt == null) {
            return;
        }
        if (mediaTypeRegistry.isSpecializationOf(mt, ZIP) || mt.equals(ZIP)) {
            processZip(f, writer);
        }

    }

    private void processZip(File f, Writer writer) {

        String fName = f.getName();
        write(writer, "file_name", fName);

        try (TikaInputStream tis = TikaInputStream.get(f)) {
            MediaType mt = zipContainerDetector.detect(tis, new Metadata());
            if (mt == null) {
                write(writer, "mime", "unknown");
            } else {
                write(writer, "mime", mt.toString());
            }
        } catch (Exception e) {

        }
        try (InputStream is = Files.newInputStream(f.toPath())) {
            try (ZipInputStream zipInputStream = new ZipInputStream(is)) {
                ZipEntry ze = zipInputStream.getNextEntry();

                while (ze != null) {
                    String name = ze.getName();
                    write(writer, "entry_name", name);
                    write(writer, "entry_extension", getExt(name));
                    String[] rootDirs = getRootDirs(name);
                    write(writer, "root1", rootDirs[0]);
                    write(writer, "root2", rootDirs[1]);
                    write(writer, "comment", ze.getComment());
                    write(writer, "zip_method", Integer.toString(ze.getMethod()));

                    write(writer, "stream_creation_time", ze.getCreationTime());
                    write(writer, "stream_modified_time", ze.getLastModifiedTime());
                    write(writer, "stream_accessed_time", ze.getLastAccessTime());
                    if (ze.getName().endsWith(".rels")) {
                        Set<String> rels = parseOOXMLRels(new CloseShieldInputStream(zipInputStream));
                        for (String r : rels) {
                            write(writer, "rel", r);
                        }
                    } else if (name.endsWith("app.xml")) {
                        AppHandler appHandler = parseAppXml(new CloseShieldInputStream(zipInputStream));
                        if (appHandler != null) {
                            write(writer, "application", appHandler.app);
                            write(writer, "appVersion", appHandler.appVersion);
                        }
                    } else if (name.endsWith(".xml") || name.endsWith("xhtml")) {
                        try {
                            QName root = new XmlRootExtractor().extractRootElement(
                                    new CloseShieldInputStream(zipInputStream));
                            write(writer, "xml_ns_local", root.getNamespaceURI() + ":" + root.getLocalPart());
                            write(writer, "xml_ns", root.getNamespaceURI());
                            write(writer, "xml_local_part", root.getLocalPart());
                        } catch (Exception e) {

                        }
                    }

                    ze = zipInputStream.getNextEntry();
                }
                writer.flush();
            } catch (Exception e) {
                e.printStackTrace();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    private void write(Writer writer, String key, FileTime value) {
        if (value == null) {
            return;
        }
        try {
            writer.write(key + "\t" + clean(value.toString()) + "\n");
        } catch (IOException e) {

        }
    }

    private void write(Writer writer, String key, String value) {
        if (StringUtils.isBlank(value)) {
            return;
        }
        try {
            writer.write(key + "\t" + clean(value) + "\n");
        } catch (IOException e) {

        }
    }

    private String getExt(String name) {
        int i = name.lastIndexOf(".");
        if (i > -1) {
            return name.substring(i);
        }
        return "";
    }

    private String clean(String value) {
        if (StringUtils.isBlank(value)) {
            return StringUtils.EMPTY;
        }
        value = value.replaceAll("\\s+", " ");
        return value;
    }

    private static String[] getRootDirs(String path) {
        int index = path.indexOf("/");
        String[] roots = new String[2];
        if (index > -1) {
            roots[0] = path.substring(0, index);
            index = path.indexOf("/", index+1);
            if (index > -1) {
                roots[1] = path.substring(0, index);
            }
        }
        return roots;
    }

    public static MediaType parseOOXMLContentTypes(InputStream is) {
        ContentTypeHandler contentTypeHandler = new ContentTypeHandler();
        try {
            XMLReaderUtils.parseSAX(is, contentTypeHandler, new ParseContext());
        } catch (SecurityException e) {
            throw e;
        } catch (Exception e) {

        }
        return contentTypeHandler.mediaType;
    }




    private static class ContentTypeHandler extends DefaultHandler {

        private MediaType mediaType = null;

        @Override
        public void startElement(String uri, String localName,
                                 String name, Attributes attrs) throws SAXException {
            for (int i = 0; i < attrs.getLength(); i++) {
                String attrName = attrs.getLocalName(i);
                if (attrName.equals("ContentType")) {
                    String contentType = attrs.getValue(i);
                    if (OOXML_CONTENT_TYPES.containsKey(contentType)) {
                        mediaType = OOXML_CONTENT_TYPES.get(contentType);
                        throw new StoppingEarlyException();
                    }

                }
            }
        }
    }
    public static AppHandler parseAppXml(InputStream is) {
        AppHandler appHandler = new AppHandler();
        try {
            XMLReaderUtils.parseSAX(is, appHandler, new ParseContext());
        } catch (SecurityException e) {
            throw e;
        } catch (Exception e) {

        }
        return appHandler;
    }




    private static class AppHandler extends DefaultHandler {

        private StringBuilder buffer = new StringBuilder();
        private String app = null;
        private String appVersion = null;

        private boolean inApp = false;
        private boolean inAppVersion = false;

        @Override
        public void startElement(String uri, String localName,
                                 String name, Attributes attrs) throws SAXException {
            if ("Application".equals(localName)) {
                inApp = true;
            } else if ("AppVersion".equals(localName)) {
                inAppVersion = true;
            }

        }

        @Override
        public void endElement(String uri, String localName,
                                 String name) throws SAXException {
            if ("Application".equals(localName)) {
                app = buffer.toString();
                inApp = false;
                buffer.setLength(0);
            } else if ("AppVersion".equals(localName)) {
                inAppVersion = false;
                appVersion = buffer.toString();
                buffer.setLength(0);
            }

        }

        @Override
        public void characters(char[] chars, int start, int len) {
            if (inApp || inAppVersion) {
                buffer.append(new String(chars, start, len));
            }
        }
    }

    private static class StoppingEarlyException extends SAXException {

    }

    public static Set<String> parseOOXMLRels(InputStream is) {
        RelsHandler relsHandler = new RelsHandler();
        try {
            XMLReaderUtils.parseSAX(is, relsHandler, new ParseContext());
        } catch (SecurityException e) {
            throw e;
        } catch (Exception e) {

        }
        return relsHandler.rels;
    }

    private static class RelsHandler extends DefaultHandler {
        Set<String> rels = new HashSet<>();
        private MediaType mediaType = null;
        @Override
        public void startElement(String uri, String localName,
                                 String name, Attributes attrs) throws SAXException {
            for (int i = 0; i < attrs.getLength(); i++) {
                String attrName = attrs.getLocalName(i);
                if (attrName.equals("Type")) {
                    String contentType = attrs.getValue(i);
                    rels.add(contentType);
                    if (OOXML_CONTENT_TYPES.containsKey(contentType)) {
                        mediaType = OOXML_CONTENT_TYPES.get(contentType);
                    }
                }
            }
        }
    }

}
