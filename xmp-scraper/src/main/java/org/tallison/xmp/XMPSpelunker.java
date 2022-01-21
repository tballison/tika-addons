package org.tallison.xmp;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.pdfbox.cos.COSArray;
import org.apache.pdfbox.cos.COSBase;
import org.apache.pdfbox.cos.COSDictionary;
import org.apache.pdfbox.cos.COSDocument;
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.cos.COSObject;
import org.apache.pdfbox.cos.COSStream;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.common.COSObjectable;

//This class walks the PDF COSDocument looking for /Metadata
public class XMPSpelunker {

    public static void main(String[] args) throws Exception {
        Path root = Paths.get(args[0]);
        processDir(root, root);
    }

    private static void processDir(Path dir, Path baseDir) {
        for (File f : dir.toFile().listFiles()) {
            if (f.isDirectory()) {
                processDir(f.toPath(), baseDir);
            } else {
                processFile(f);
            }
        }
    }

    public static void processFile(File f) {
        try (PDDocument pdDocument = PDDocument.load(f)) {
            processDoc(pdDocument);

        } catch (Exception e) {

        }
    }

    private static void processDoc(PDDocument pdDocument) {
        COSDocument cosDocument = pdDocument.getDocument();

        processObjects(cosDocument.getObjects());
    }

    private static void processObjects(List<COSObject> objects) {
        for (COSObject obj : objects) {
            COSBase cosBase = obj.getObject();
            processBase(cosBase);
        }
    }

    private static void processBase(COSBase cosBase) {
        if (cosBase instanceof COSDictionary) {
            processDictionary((COSDictionary)cosBase);
        } else if (cosBase instanceof COSStream) {
            processStream((COSStream)cosBase);
        } else if (cosBase instanceof COSArray) {
            processArray((COSArray) cosBase);
        } else if (cosBase instanceof COSObject) {
            processObject((COSObject)cosBase);
        } else {
            System.out.println("skipping "+cosBase.getClass());
        }
    }

    private static void processObject(COSObject cosBase) {
        System.out.println("processing object "+cosBase.getObjectNumber());
        processBase(cosBase.getObject());
    }

    private static void processArray(COSArray cosArray) {
        for (COSBase base : cosArray) {
            processBase(base);
        }
    }

    private static void processStream(COSStream cosStream) {
        Set<COSName> names = cosStream.keySet();
        System.out.println("Stream names");
        for (COSName n : names) {
            System.out.print(n);
            System.out.print(" ");
        }
        System.out.println("");

    }

    private static void processDictionary(COSDictionary cosDictionary) {
        for (Map.Entry<COSName, COSBase> e : cosDictionary.entrySet()) {
            System.out.println(e.getKey().toString() + " : " +e.getValue());
            System.out.println("processing "+e.getKey().getName());
            processBase(e.getValue());
        }
    }
}
