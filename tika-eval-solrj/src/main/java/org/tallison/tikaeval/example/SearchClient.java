package org.tallison.tikaeval.example;

import org.apache.solr.client.solrj.SolrServerException;
import org.apache.tika.metadata.Metadata;

import java.io.Closeable;
import java.io.IOException;

public interface SearchClient extends Closeable {

    void deleteAll() throws IOException;

    void addDoc(Metadata metadata) throws IOException;

    //this is slightly more complicated, but this will suffice
    String getIdField();
}
