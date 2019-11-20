package org.tallison.tikaeval.example;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.tika.io.IOExceptionWithCause;
import org.apache.tika.metadata.Metadata;

import java.io.IOException;

public class SolrJClient implements SearchClient {
    private final SolrClient solrJClient;
    public SolrJClient(String solrUrl) throws SearchClientException {
        solrJClient =
                new ConcurrentUpdateSolrClient.Builder(solrUrl).build();
    }

    @Override
    public void deleteAll() throws IOException {
        try {
            solrJClient.deleteByQuery("*:*");
            solrJClient.commit();
        } catch (SolrServerException e) {
            throw new IOExceptionWithCause(e);
        }
    }

    @Override
    public void addDoc(Metadata metadata) throws IOException {
        SolrInputDocument doc = new SolrInputDocument();
        for (String n : metadata.names()) {
            String[] values = metadata.getValues(n);
            if (values.length == 1) {
                doc.setField(n, values[0]);
            } else {
                doc.setField(n, values);
            }
        }
        try {
            solrJClient.add(doc, 1000);
        } catch (SolrServerException e) {
            throw new IOExceptionWithCause(e);
        }
    }

    @Override
    public String getIdField() {
        return "id";
    }

    @Override
    public void close() throws IOException {
        try {
            solrJClient.commit();
        } catch (SolrServerException e) {
            throw new IOExceptionWithCause(e);
        }
        solrJClient.close();
    }
}
