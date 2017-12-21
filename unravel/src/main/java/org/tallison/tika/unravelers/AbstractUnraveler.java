package org.tallison.tika.unravelers;

import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.ContentHandlerFactory;

public abstract class AbstractUnraveler extends AbstractParser {

    protected final PostParseHandler postParseHandler;
    private final Parser parser;
    private final ContentHandlerFactory contentHandlerFactory;
    public AbstractUnraveler(Parser parser, ContentHandlerFactory contentHandlerFactory,
                        PostParseHandler postParseHandler) {
        this.postParseHandler = postParseHandler;
        this.parser = parser;
        this.contentHandlerFactory = contentHandlerFactory;
    }

    protected MyRecursiveParserWrapper getRecursiveParserWrapper() {
        return new MyRecursiveParserWrapper(parser, contentHandlerFactory);
    }

}
