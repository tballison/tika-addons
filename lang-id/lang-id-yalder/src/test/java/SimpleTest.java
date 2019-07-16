import java.util.List;

import org.junit.Test;
import org.tallison.langid.LangDetectResult;
import org.tallison.langid.yalder.YalderDetector;

public class SimpleTest {

    @Test
    public void testSimple() throws Exception {
        double d = 2.0;
        for (int i = 1; i < 1000; i++) {
            System.out.println(d /= i);
        }/*
        YalderDetector d = new YalderDetector();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 3; i++) {
            sb.append("four score and seven years ago ");
        }
        for (int i = 0; i < 100; i++) {
            sb.append("La MEL réunit 90 communes sur un territoire de près de 650 km2 où résident plus de 1,1 million d’habitants. Située au centre d'une aire géographique très densément peuplée, à l’extrême ouest de la plaine d'Europe du Nord, elle est encadrée");
        }
        List<LangDetectResult> results =
                d.detect(sb.toString());
        System.out.println(results);
*/
    }
}
