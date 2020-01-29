package org.tallison.indexers;

import java.io.File;

public class CurlIndexer {

    public static void main(String[] args) throws Exception {
        File fileDir = new File("/home/tim/data/tika-test-documents");

        int id = 0;
        for (File f : fileDir.listFiles()) {
            if (f.isDirectory()) {
                continue;
            }
            if (! f.getName().equals("")) {
               // continue;
            }
            try {
                System.out.println("trying "+f);
                String path = f.getAbsolutePath();
                String myFile = "";
                if (path.contains(" ")) {
                    myFile = "\"myFile=@"+f.getAbsolutePath()+"\"";
                } else {
                    myFile = "myFile=@"+f.getAbsolutePath();
                }
                ProcessBuilder processBuilder = new ProcessBuilder(
                        "curl",
                        "http://localhost:8983/solr/tika-integration-example-9.x/update/extract?literal.id=doc"+id+"&commit=true",
                        "-F",
                        myFile);
                processBuilder.inheritIO();
                Process p = processBuilder.start();
                p.waitFor();
                System.out.println(p.exitValue());
                id++;
            } catch (Exception e) {
                e.printStackTrace();;
            }
        }

    }

}
