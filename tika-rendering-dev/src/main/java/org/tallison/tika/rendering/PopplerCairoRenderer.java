package org.tallison.tika.rendering;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.utils.FileProcessResult;
import org.apache.tika.utils.ProcessUtils;

public class PopplerCairoRenderer implements Renderer {

    @Override
    public RenderResults render(InputStream is, ParseContext parseContext)
            throws IOException, TikaException {
        TikaInputStream tis = TikaInputStream.get(is);
        TemporaryResources tmp = new TemporaryResources();
        Path dir = Files.createTempDirectory("tika-render-");
        //TODO -- this assumes files have been deleted first
        //do something smarter
        tmp.addResource(new Closeable() {
            @Override
            public void close() throws IOException {
                Files.delete(dir);
            }
        });
        //TODO -- run mutool pages to get page sizes
        //and then use that information in the -O to get proper scaling
        //etc.
        // This would also allow us to run on a single page at a time if that's of any interest

        String[] args = new String[] {
                "pdftocairo",
                "-png",
                "-gray",
                ProcessUtils.escapeCommandLine(tis.getPath().toAbsolutePath().toString()),
                ProcessUtils.escapeCommandLine(dir.toAbsolutePath().toString()+"/"+
                        "tika-cairo-render"),

        };
        ProcessBuilder builder = new ProcessBuilder();
        builder.command(args);
        //TODO: parameterize timeout
        FileProcessResult result = ProcessUtils.execute(builder, 60000, 10, 1000);
        if (result.getExitValue() != 0) {
            throw new TikaException(result.getStderr());
        }
        RenderResults results = new RenderResults(tmp);
        //TODO -- fix this
        Matcher m = Pattern.compile("tika-cairo-render-(\\d+)\\.png").matcher("");
        for (File f : dir.toFile().listFiles()) {
            String n = f.getName();
            if (m.reset(n).find()) {
                int pageIndex = Integer.parseInt(m.group(1));
                Metadata metadata = new Metadata();
                metadata.set(PDFBoxRenderer.PAGE_NUMBER, pageIndex);
                results.add(new RenderResult(f.toPath(), metadata));
            }
        }

        return results;
    }
}
