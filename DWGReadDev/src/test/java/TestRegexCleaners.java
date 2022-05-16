import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

public class TestRegexCleaners {

    @Test
    public void testBasic() throws Exception {
        String formatted = "\\A1;\\fAIGDT|b0|i0;\\H2.5000;\\ln\\fArial|b0|i0;\\H2.5000;68{\\H1.3;\\S+0,8^+0,1;}";

        String expected = "n68+0,8/+0,1";
        assertEquals(expected, clean(formatted));
    }

    @Test
    public void testParameterizables() throws Exception {
        String formatted = "the quick \\A1;\\fAIGDT|b0|i0;\\H2.5000; brown fox";
        String expected = "the quick  brown fox";
        assertEquals(expected, clean(formatted));
    }

    @Test
    public void testUnderlineEtc() throws Exception {
        String formatted = "l \\L open cu\\lrly bra\\Kck\\ket \\\\{ and a close " +
                "\\\\} right?";
        String expected = "l  open curly bracket { and a close } right?";
        assertEquals(expected, clean(formatted));

    }
    @Test
    public void testEscaped() throws Exception {
        String formatted = "then an actual \\P open curly bracket \\{ and a close \\} right?";
        String expected = "then an actual \n open curly bracket { and a close } right?";
        assertEquals(expected, clean(formatted));
    }

    @Test
    public void testStackedFractions() throws Exception {
        String formatted = "abc \\S+0,8^+0,1; efg";
        String expected = "abc +0,8/+0,1 efg";
        assertEquals(expected, clean(formatted));
    }

    private String clean(String formatted) {
        String txt = formatted;
        StringBuffer sb = new StringBuffer();
        //Strip off start/stop underline/overstrike/strike throughs
        Matcher m = Pattern.compile("((?:\\\\\\\\)+|(?:\\\\[LlOoKk]))").matcher(txt);
        while (m.find()) {
            if (! m.group(1).endsWith("\\")) {
                m.appendReplacement(sb, "");
            }
        }
        m.appendTail(sb);
        txt = sb.toString();

        //Strip off semi-colon ended markers
        m =
                Pattern.compile("((?:\\\\\\\\)+|(?:\\\\(?:A|H|pi|pxt|pxi|X|Q|H|f|W|C|T)[^;]{0," +
                        "100};))").matcher(txt);
        sb.setLength(0);
        while (m.find()) {
            if (! m.group(1).endsWith("\\")) {
                m.appendReplacement(sb, "");
            }
        }
        m.appendTail(sb);
        txt = sb.toString();

        //new line marker \\P replace with actual new line
        m = Pattern.compile("((?:\\\\\\\\)+|(?:\\\\P))").matcher(txt);
        sb.setLength(0);
        while (m.find()) {
            if (m.group(1).endsWith("P")) {
                m.appendReplacement(sb, "\n");
            }
        }
        m.appendTail(sb);
        txt = sb.toString();

        //stacking fractions
        m = Pattern.compile("(?:(\\\\\\\\)+|(?:\\\\S([^/^#]{1,20})[/^#]([^;]{1,20});))").matcher(txt);
        sb.setLength(0);
        while (m.find()) {
            if (m.group(1) == null) {;
                m.appendReplacement(sb, m.group(2) + "/" + m.group(3));
            }
        }
        m.appendTail(sb);
        txt = sb.toString();

        //strip brackets around text, make sure they aren't escaped
        m = Pattern.compile("(\\\\)+[{}]|([{}])").matcher(txt);
        sb.setLength(0);
        while (m.find()) {
            if (m.group(1) == null) {;
                m.appendReplacement(sb, "");
            }
        }
        m.appendTail(sb);
        txt = sb.toString();
        //now get rid of escape characters
        txt = txt.replaceAll("\\\\", "");
        return txt;
    }
}
