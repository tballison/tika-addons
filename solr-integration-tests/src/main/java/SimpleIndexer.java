
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.ContentStreamUpdateRequest;

import java.io.File;

public class SimpleIndexer {

    public static void main(String[] args) throws Exception {
        File fileDir = new File("/home/tim/data/tika-test-documents");
        SolrClient client = new HttpSolrClient.Builder(
                "http://localhost:8983/solr/tika-integration-example").build();
        for (File f : fileDir.listFiles()) {
            if (f.isDirectory()) {
                continue;
            }
            try {
                System.out.println("trying "+f);
                ContentStreamUpdateRequest req = new ContentStreamUpdateRequest("/update/extract");
                req.addFile(f, "");
                req.setParam("literal.id", f.getName());
                req.setParam("fmap.content", "text");
                System.out.println("result " + client.request(req));
            } catch (Exception e) {
                e.printStackTrace();;
            }
        }

    }

}
