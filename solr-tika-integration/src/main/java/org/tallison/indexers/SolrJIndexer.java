package org.tallison.indexers;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;

        import java.io.File;
import java.util.Arrays;

public class SolrJIndexer {

    public static void main(String[] args) throws Exception {
        File fileDir = new File("/home/tim/data/tika-test-documents");
        SolrClient client = new HttpSolrClient.Builder(
                "http://localhost:8983/solr/tika-integration-example-8.x").build();
        client.deleteByQuery("*:*");
        client.commit();
        File[] files = fileDir.listFiles();
        Arrays.sort(files);
        for (File f : files) {
            if (f.isDirectory()) {
                continue;
            }
            if (!f.getName().equals("test-documents.7z")) {
                //continue;
            }
            for (int i = 0; i < 1; i++) {
                try {
                    System.out.println("trying " + f);
                    ContentStreamUpdateRequest req = new ContentStreamUpdateRequest("/update/extract");
                    req.addFile(f, "");
                    req.setParam("literal.id", f.getName());
                    req.setParam("fmap.content", "text");
                    System.out.println("result " + client.request(req));
                } catch (Exception e) {
                    e.printStackTrace();
                    ;
                }
            }
        }
        client.commit();
        client.close();

    }

}
