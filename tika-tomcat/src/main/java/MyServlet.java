import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;

import org.apache.commons.io.IOUtils;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.CompositeDetector;
import org.apache.tika.detect.Detector;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.ToTextContentHandler;
import org.apache.tika.sax.ToXMLContentHandler;

@WebServlet("/MyServlet")
public class MyServlet extends HttpServlet {


    @Override
    protected void doPut(HttpServletRequest request, HttpServletResponse response) throws
            ServletException,
            IOException {
        response.setContentType("text/html");
        response.setCharacterEncoding("UTF-8");

        Path tmpFile = Files.createTempFile("my-tmp", "");
        TikaConfig config = null;
        try (InputStream is = this.getClass().getResourceAsStream("/my-tika-config.xml")) {
            config = new TikaConfig(is);
        } catch (TikaException|SAXException e) {
            throw new RuntimeException(e);
        }
        AutoDetectParser parser = new AutoDetectParser(config);

        ContentHandler contentHandler = new ToTextContentHandler();
        String txt = "";
        try {
            Files.copy(request.getInputStream(), tmpFile, StandardCopyOption.REPLACE_EXISTING);
            try (InputStream tis = TikaInputStream.get(tmpFile)) {
                parser.parse(tis, contentHandler, new Metadata(), new ParseContext());
            } catch (TikaException e) {
                e.printStackTrace();
            } catch (SAXException e) {
                e.printStackTrace();
            }
            txt = contentHandler.toString();
            System.out.println(contentHandler.toString());
        } finally {
            Files.delete(tmpFile);
        }

        System.out.println("do posting");
        try (PrintWriter writer = response.getWriter()) {
            writer.println("<!DOCTYPE html><html>");
            writer.println("<head>");
            writer.println("<meta charset=\"UTF-8\" />");
            writer.println("<title>MyServlet.java:doGet(): Servlet code!</title>");
            writer.println("</head>");
            writer.println("<body>");

            writer.println("<h1>This is a simple java servlet.</h1>");
            debugDetectors(config.getDetector(), writer, 0);
            writer.println("<p>");
            writer.println(txt);
            writer.println("</p>");

            writer.println("</body>");
            writer.println("</html>");
        }
    }

    private void debugDetectors(Detector detector, PrintWriter printWriter, int depth) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < depth; i++) {
            sb.append("\t");
        }
        printWriter.println(sb.toString() + "detector: " + detector);
        if (detector instanceof CompositeDetector) {
            for (Detector d : ((CompositeDetector)detector).getDetectors()) {
                debugDetectors(d, printWriter, (depth+1));
            }
        }
    }
}
