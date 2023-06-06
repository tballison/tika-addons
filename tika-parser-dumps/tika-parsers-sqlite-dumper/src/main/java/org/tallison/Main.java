package org.tallison;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.mime.MediaType;
import org.apache.tika.mime.MediaTypeRegistry;
import org.apache.tika.mime.MimeTypeException;
import org.apache.tika.mime.MimeTypes;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.CompositeParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;

public class Main {
    public static void main(String[] args) {
        TikaConfig tikaConfig = TikaConfig.getDefaultConfig();
        MimeTypes mimeTypes = tikaConfig.getMimeRepository();
        Parser defaultParser = tikaConfig.getParser();
        Set<Parser> seen = new HashSet<>();
        System.out.println("<dependency>\n" + "    <groupId>org.apache.tika</groupId>\n" +
                "    <artifactId>tika-parser-sqlite3-package</artifactId>\n" +
                "    <version>2.8.0</version>\n" + "    <scope>test</scope>\n" + "</dependency>\n");

        System.out.println("|Parser Class|mime-type|extension|");
        System.out.println("|_____________|___________|__________|");
        List<Parser> parsers = new ArrayList<>();
        for (Map.Entry<MediaType, Parser> e :
                ((CompositeParser)defaultParser).getParsers().entrySet()) {
            if (seen.contains(e.getValue())) {
                continue;
            }
            parsers.add(e.getValue());
            seen.add(e.getValue());
        }
        Collections.sort(parsers, new Comparator<Parser>() {
            @Override
            public int compare(Parser o1, Parser o2) {
                return o1.getClass().getName().compareTo(o2.getClass().getName());
            }
        });
        for (Parser p : parsers) {
            for (MediaType t : p.getSupportedTypes(new ParseContext())) {
                String ext = "";
                try {
                    ext = mimeTypes.forName(t.toString()).getExtension();
                } catch (MimeTypeException ex) {
                    ex.printStackTrace();
                    //swallow
                }
                System.out.println(p.getClass().getName() + "\t" + t + "\t" + ext);

            }
        }
    }
}