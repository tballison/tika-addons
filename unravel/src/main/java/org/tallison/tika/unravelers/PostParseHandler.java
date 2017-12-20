package org.tallison.tika.unravelers;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;

import java.io.IOException;
import java.util.List;

public interface PostParseHandler {

    public void handle(List<Metadata> metadataList) throws TikaException, IOException;
}
