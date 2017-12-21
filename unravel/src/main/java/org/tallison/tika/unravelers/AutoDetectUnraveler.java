package org.tallison.tika.unravelers;

import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.pkg.CompressorParserOptions;
import org.apache.tika.sax.ContentHandlerFactory;
import org.tallison.tika.unravelers.mbox.MBoxUnraveler;
import org.tallison.tika.unravelers.pkg.PackageUnraveler;
import org.tallison.tika.unravelers.pst.PSTUnraveler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;


public class AutoDetectUnraveler extends AutoDetectParser {
    private int memoryLimitInKb = 100000;
    public AutoDetectUnraveler() {
        throw new UnsupportedOperationException("need to use the initializer with the postparsehandler and recursiveparserwrapper");
    }

    public AutoDetectUnraveler(Parser parser, ContentHandlerFactory contentHandlerFactory,
                               PostParseHandler postParseHandler) {
        super(new PSTUnraveler(parser, contentHandlerFactory, postParseHandler),
                new MBoxUnraveler(parser, contentHandlerFactory, postParseHandler),
                new PackageUnraveler(parser, contentHandlerFactory, postParseHandler));
    }

    @Override
    public void parse(InputStream is, ContentHandler contentHandler, Metadata metadata, ParseContext context)
            throws SAXException, IOException, TikaException {
        if (! is.markSupported()) {
            is = TikaInputStream.get(is);
        }

        InputStream stream = decompressStream(is, metadata, context);
        super.parse(stream, contentHandler, metadata, context);
    }

    private InputStream decompressStream(InputStream is, Metadata metadata, ParseContext context) throws TikaException {
        try {
            if (! is.markSupported()) {
                is = TikaInputStream.get(is);
            }
            String name = CompressorStreamFactory.detect(is);
            CompressorParserOptions options =
                    context.get(CompressorParserOptions.class, new CompressorParserOptions() {
                        public boolean decompressConcatenated(Metadata metadata) {
                            return false;
                        }
                    });
            CompressorStreamFactory factory =
                    new CompressorStreamFactory(options.decompressConcatenated(metadata), memoryLimitInKb);
            //recursively try to decompress the stream
            return decompressStream(factory.createCompressorInputStream(name, is), metadata, context);
        } catch (CompressorException e) {
            if (e.getMessage() != null && e.getMessage().contains("No Compressor found")) {
                return is;
            } else {
                throw new TikaException("compressor exception", e);
            }
        }

    }

    public void setMemoryLimitInKb(int memoryLimitInKb) {
        this.memoryLimitInKb = memoryLimitInKb;
    }
}



