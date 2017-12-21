package org.tallison.tika.unravelers.pkg;


import org.apache.commons.compress.PasswordRequiredException;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.archivers.StreamingNotSupportedException;
import org.apache.commons.compress.archivers.ar.ArArchiveInputStream;
import org.apache.commons.compress.archivers.cpio.CpioArchiveInputStream;
import org.apache.commons.compress.archivers.dump.DumpArchiveInputStream;
import org.apache.commons.compress.archivers.jar.JarArchiveInputStream;
import org.apache.commons.compress.archivers.sevenz.SevenZFile;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.zip.UnsupportedZipFeatureException;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.io.input.CloseShieldInputStream;
import org.apache.tika.exception.EncryptedDocumentException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.PasswordProvider;
import org.apache.tika.sax.ContentHandlerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.tika.unravelers.AbstractUnraveler;
import org.tallison.tika.unravelers.MyRecursiveParserWrapper;
import org.tallison.tika.unravelers.PostParseHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.Set;

public class PackageUnraveler extends AbstractUnraveler {

    private static final Logger LOG = LoggerFactory.getLogger(PackageUnraveler.class);

    /**
     * Serial version UID
     */
    private static final long serialVersionUID = -5331043266963888708L;

    private static final MediaType ZIP = MediaType.APPLICATION_ZIP;
    private static final MediaType JAR = MediaType.application("java-archive");
    private static final MediaType AR = MediaType.application("x-archive");
    private static final MediaType ARJ = MediaType.application("x-arj");
    private static final MediaType CPIO = MediaType.application("x-cpio");
    private static final MediaType DUMP = MediaType.application("x-tika-unix-dump");
    private static final MediaType TAR = MediaType.application("x-tar");
    private static final MediaType SEVENZ = MediaType.application("x-7z-compressed");

    private static final Set<MediaType> SUPPORTED_TYPES =
            MediaType.set(ZIP, JAR, AR, ARJ, CPIO, DUMP, TAR, SEVENZ);


    @Deprecated
    static MediaType getMediaType(ArchiveInputStream stream) {
        if (stream instanceof JarArchiveInputStream) {
            return JAR;
        } else if (stream instanceof ZipArchiveInputStream) {
            return ZIP;
        } else if (stream instanceof ArArchiveInputStream) {
            return AR;
        } else if (stream instanceof CpioArchiveInputStream) {
            return CPIO;
        } else if (stream instanceof DumpArchiveInputStream) {
            return DUMP;
        } else if (stream instanceof TarArchiveInputStream) {
            return TAR;
        } else if (stream instanceof SevenZWrapper) {
            return SEVENZ;
        } else {
            return MediaType.OCTET_STREAM;
        }
    }

    static MediaType getMediaType(String name) {
        if (ArchiveStreamFactory.JAR.equals(name)) {
            return JAR;
        } else if (ArchiveStreamFactory.ZIP.equals(name)) {
            return ZIP;
        } else if (ArchiveStreamFactory.AR.equals(name)) {
            return AR;
        } else if (ArchiveStreamFactory.ARJ.equals(name)) {
            return ARJ;
        } else if (ArchiveStreamFactory.CPIO.equals(name)) {
            return CPIO;
        } else if (ArchiveStreamFactory.DUMP.equals(name)) {
            return DUMP;
        } else if (ArchiveStreamFactory.TAR.equals(name)) {
            return TAR;
        } else if (ArchiveStreamFactory.SEVEN_Z.equals(name)) {
            return SEVENZ;
        } else {
            return MediaType.OCTET_STREAM;
        }
    }

    public Set<MediaType> getSupportedTypes(ParseContext context) {
        return SUPPORTED_TYPES;
    }

    public PackageUnraveler(Parser parser, ContentHandlerFactory contentHandlerFactory, PostParseHandler postParseHandler) {
        super(parser, contentHandlerFactory, postParseHandler);
    }

    @Override
    public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context) throws IOException, SAXException, TikaException {
        // Ensure that the stream supports the mark feature
        if (!stream.markSupported()) {
            stream = new BufferedInputStream(stream);
        }

        MyRecursiveParserWrapper recursiveParserWrapper = getRecursiveParserWrapper();
        TemporaryResources tmp = new TemporaryResources();
        ArchiveInputStream ais = null;
        try {
            ArchiveStreamFactory factory = context.get(ArchiveStreamFactory.class, new ArchiveStreamFactory());
            // At the end we want to close the archive stream to release
            // any associated resources, but the underlying document stream
            // should not be closed

            ais = factory.createArchiveInputStream(new CloseShieldInputStream(stream));

        } catch (StreamingNotSupportedException sne) {
            // Most archive formats work on streams, but a few need files
            if (sne.getFormat().equals(ArchiveStreamFactory.SEVEN_Z)) {
                // Rework as a file, and wrap
                stream.reset();
                TikaInputStream tstream = TikaInputStream.get(stream, tmp);

                // Seven Zip suports passwords, was one given?
                String password = null;
                PasswordProvider provider = context.get(PasswordProvider.class);
                if (provider != null) {
                    password = provider.getPassword(metadata);
                }

                SevenZFile sevenz;
                if (password == null) {
                    sevenz = new SevenZFile(tstream.getFile());
                } else {
                    sevenz = new SevenZFile(tstream.getFile(), password.getBytes("UnicodeLittleUnmarked"));
                }

                // Pending a fix for COMPRESS-269 / TIKA-1525, this bit is a little nasty
                ais = new SevenZWrapper(sevenz);
            } else {
                tmp.close();
                throw new TikaException("Unknown non-streaming format " + sne.getFormat(), sne);
            }
        } catch (ArchiveException e) {
            tmp.close();
            throw new TikaException("Unable to unpack document stream", e);
        }

        try {
            ArchiveEntry entry = ais.getNextEntry();
            while (entry != null) {
                if (!entry.isDirectory()) {
                    parseEntry(ais, entry, recursiveParserWrapper);
                }
                entry = ais.getNextEntry();
            }
        } catch (UnsupportedZipFeatureException zfe) {
            // If it's an encrypted document of unknown password, report as such
            if (zfe.getFeature() == UnsupportedZipFeatureException.Feature.ENCRYPTION) {
                throw new EncryptedDocumentException(zfe);
            }
            // Otherwise throw the exception
            throw new TikaException("UnsupportedZipFeature", zfe);
        } catch (PasswordRequiredException pre) {
            throw new EncryptedDocumentException(pre);
        } finally {
            ais.close();
            tmp.close();
        }

    }


    private void parseEntry(
            ArchiveInputStream archive, ArchiveEntry entry,
            MyRecursiveParserWrapper recursiveParserWrapper)
            throws SAXException, IOException, TikaException {
        String name = entry.getName();
        if (archive.canReadEntryData(entry)) {
            // Fetch the metadata on the entry contained in the archive
            Metadata entrydata = handleEntryMetadata(name, null,
                    entry.getLastModifiedDate(), entry.getSize());
                TemporaryResources tmp = new TemporaryResources();
                InputStream tis = TikaInputStream.get(archive, tmp);
                try {
                    recursiveParserWrapper.parse(tis, new DefaultHandler(), entrydata, new ParseContext());
                } finally {
                    tmp.close();
                }
                try {
                    postParseHandler.handle(recursiveParserWrapper.getMetadata());
                } finally {
                    recursiveParserWrapper.reset();
                }
        } else {

        }
    }

    protected static Metadata handleEntryMetadata(
            String name, Date createAt, Date modifiedAt,
            Long size)
            throws SAXException, IOException, TikaException {
        Metadata entrydata = new Metadata();
        if (createAt != null) {
            entrydata.set(TikaCoreProperties.CREATED, createAt);
        }
        if (modifiedAt != null) {
            entrydata.set(TikaCoreProperties.MODIFIED, modifiedAt);
        }
        if (size != null) {
            entrydata.set(Metadata.CONTENT_LENGTH, Long.toString(size));
        }
        if (name != null && name.length() > 0) {
            name = name.replace("\\", "/");
            entrydata.set(Metadata.RESOURCE_NAME_KEY, name);
            entrydata.set(Metadata.EMBEDDED_RELATIONSHIP_ID, name);
        }
        return entrydata;
    }

    // Pending a fix for COMPRESS-269, we have to wrap ourselves
    private static class SevenZWrapper extends ArchiveInputStream {
        private SevenZFile file;

        private SevenZWrapper(SevenZFile file) {
            this.file = file;
        }

        @Override
        public int read() throws IOException {
            return file.read();
        }

        @Override
        public int read(byte[] b) throws IOException {
            return file.read(b);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return file.read(b, off, len);
        }

        @Override
        public ArchiveEntry getNextEntry() throws IOException {
            return file.getNextEntry();
        }

        @Override
        public void close() throws IOException {
            file.close();
        }
    }
}