package org.tallison.langid.optimaize;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import com.optimaize.langdetect.DetectedLanguage;
import com.optimaize.langdetect.LanguageDetector;
import com.optimaize.langdetect.LanguageDetectorBuilder;
import com.optimaize.langdetect.ngram.NgramExtractors;
import com.optimaize.langdetect.profiles.LanguageProfile;
import com.optimaize.langdetect.profiles.LanguageProfileReader;
import com.optimaize.langdetect.text.MultiTextFilter;
import com.optimaize.langdetect.text.RemoveMinorityScriptsTextFilter;
import com.optimaize.langdetect.text.TextFilter;
import com.optimaize.langdetect.text.TextObject;
import com.optimaize.langdetect.text.TextObjectFactory;
import org.tallison.langid.LangDetectResult;
import org.tallison.langid.LangDetector;

public class OptimaizeLangDetector implements LangDetector {
    static List<LanguageProfile> languageProfiles;
    static LanguageDetector detector;
    static TextObjectFactory textObjectFactory;

    static int MAX_TEXT_LENGTH = 50000;
    static Map<String, String> TWO_TO_THREE = new ConcurrentHashMap<>();
    static {
        try {
            loadBuiltInModels();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        String[] mappings = new String[]{
                "af", "afr",
                "ar", "ara",
                "ast", "ast",
                "be", "bel",
                "bn", "ben",
                "br", "bre",
                "bg", "bul",
                "ca", "cat",
                "cs", "ces",
                "cy", "cym",
                "da", "dan",
                "de", "deu",
                "el", "ell",
                "en", "eng",
                "et", "est",
                "eu", "eus",
                "fa", "fas",
                "fi", "fin",
                "fr", "fra",
                "ga", "gle",
                "gl", "glg",
                "gu", "guj",
                "he", "heb",
                "hi", "hin",
                "hr", "hrv",
                "ht", "hat",
                "hu", "hun",
                "id", "ind",
                "is", "isl",
                "it", "ita",
                "ja", "jpn",
                "kn", "kan",
                "ko", "kor",
                "lv", "lav",
                "lt", "lit",
                "lv", "lvs",
                "ml", "mal",
                "mr", "mar",
                "mk", "mkd",
                "mt", "mlt",
                "ms", "msa",
                "ne", "nep",
                "nl", "nld",
                "no", "nno",//nynorsk
                "no", "nob",//bokmal
                "oc", "oci",
                "pa", "pan",
                "pl", "pol",
                "pt", "por",
                "ro", "ron",
                "ru", "rus",
                "sk", "slk",
                "sl", "slv",
                "so", "som",
                "es", "spa",
                "sq", "sqi",
                "sr", "srp",
                //id -> sunid?
                "sw", "swa",
                "sv", "swe",
                "ta", "tam",
                "te", "tel",
                "tl", "tgl",
                "th", "tha",
                "tr", "tur",
                "uk", "ukr",
                "ur", "urd",
                "vi", "vie",
                "zh-CN", "cmn",
                "zh-TW", "cmn"

        };
        for (int i = 0; i < mappings.length-1; i += 1) {
            TWO_TO_THREE.put(mappings[i], mappings[i+1]);
        }
    }

    public static void loadBuiltInModels() throws IOException {

        languageProfiles = new LanguageProfileReader().readAllBuiltIn();
        detector = LanguageDetectorBuilder.create(NgramExtractors.standard())
                .withProfiles(languageProfiles)
                .build();
        textObjectFactory = buildTextObjectFactory();
    }

    private Set<String> supportedLangs = null;

    public OptimaizeLangDetector() {
        Set<String> tmp = new HashSet<>();
        for (LanguageProfile lang : languageProfiles) {
            String lng = TWO_TO_THREE.get(lang.getLocale().toString());
            if (lng == null) {
                lng = lang.getLocale().toString();
            }
            tmp.add(lng);
        }
        supportedLangs = Collections.unmodifiableSet(tmp);
    }
    @Override
    public Set<String> getSupportedLangs() {
        return supportedLangs;
    }

    @Override
    public List<LangDetectResult> detect(String s) {
        List<DetectedLanguage> results = detector.getProbabilities(s);
        List<LangDetectResult> ret = new ArrayList<>();
        for (DetectedLanguage lang : results) {
            String langString = lang.getLocale().toString();
            if (TWO_TO_THREE.containsKey(langString)) {
                langString = TWO_TO_THREE.get(langString);
            }
            ret.add(new LangDetectResult(langString, lang.getProbability()));
        }
        return ret;
    }

    private static TextObjectFactory buildTextObjectFactory() {
        List<TextFilter> textFilters = new ArrayList<>();
        textFilters.add(TikasUrlTextFilter.getInstance());
        textFilters.add(RemoveMinorityScriptsTextFilter.forThreshold(0.3));
        return new TextObjectFactory(new MultiTextFilter(textFilters), MAX_TEXT_LENGTH);
    }



    public static List<DetectedLanguage> getProbabilities(String s) {
        TextObject textObject = textObjectFactory.forText(s);
        return detector.getProbabilities(textObject);
    }

    public static void setMaxTextLength(int maxTextLength) {
        MAX_TEXT_LENGTH = maxTextLength;
    }

    private static class TikasUrlTextFilter implements TextFilter {
        //use this custom copy/paste of optimaize to avoid long, long hang with mail_regex
        //TIKA-2777
        private static final Pattern URL_REGEX = Pattern.compile("https?://[-_.?&~;+=/#0-9A-Za-z]{10,10000}");
        private static final Pattern MAIL_REGEX = Pattern.compile("[-_.0-9A-Za-z]{1,100}@[-_0-9A-Za-z]{1,100}[-_.0-9A-Za-z]{1,100}");
        private static final TikasUrlTextFilter INSTANCE = new TikasUrlTextFilter();

        public static TikasUrlTextFilter getInstance() {
            return INSTANCE;
        }

        private TikasUrlTextFilter() {
        }

        public String filter(CharSequence text) {
            String modified = URL_REGEX.matcher(text).replaceAll(" ");
            return MAIL_REGEX.matcher(modified).replaceAll(" ");
        }
    }
}
