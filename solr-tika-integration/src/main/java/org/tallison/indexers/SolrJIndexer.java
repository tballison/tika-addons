package org.tallison.indexers;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;

        import java.io.File;
import java.util.Arrays;

public class SolrJIndexer {

    public static void main(String[] args) throws Exception {
        File fileDir = new File("/home/tallison/data/tika-test-documents");
        SolrClient client = new HttpSolrClient.Builder(
                "http://localhost:8983/solr/tika-integration-example").build();
        client.deleteByQuery("*:*", 100);

        File[] files = fileDir.listFiles();
        Arrays.sort(files);
        int success = 0;
        int failed = 0;
        int total = files.length;
        for (File f : files) {
            if (f.isDirectory()) {
                continue;
            }
            if (!f.getName().equals("test5.PNG.tsd")) {
                //continue;
            }
            for (int i = 0; i < 1; i++) {
                try {
                    //System.out.println("trying " + f);
                    ContentStreamUpdateRequest req = new ContentStreamUpdateRequest("/update/extract");
                    req.addFile(f, "");
                    req.setParam("literal.id", f.getName());
                    req.setParam("fmap.content", "text");
                    System.out.println("result " +f + " : " + client.request(req));
                    success++;
                } catch (Exception e) {
                    System.err.println("bad file: "+f);
                    e.printStackTrace();
                    failed++;
                }
            }
            System.out.println("success: "+success+" failed: "+failed+
                    " success+failed: "+(success+failed)+" total: "+total);

        }
        client.commit();
        client.close();
        System.out.println("success: "+success+" failed: "+failed+
                " success+failed: "+(success+failed)+" total: "+total);
    }

}
