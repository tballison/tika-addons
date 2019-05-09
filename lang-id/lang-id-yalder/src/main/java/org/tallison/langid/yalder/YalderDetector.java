package org.tallison.langid.yalder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.krugler.yalder.BaseLanguageModel;
import org.krugler.yalder.DetectionResult;
import org.krugler.yalder.ModelLoader;
import org.krugler.yalder.hash.HashLanguageDetector;
import org.tallison.langid.LangDetectResult;
import org.tallison.langid.LangDetector;

public class YalderDetector implements LangDetector {

    Collection<BaseLanguageModel> models;
    HashLanguageDetector detector;
    Set<String> supportedLangs;
    public YalderDetector() throws IOException {
        models = ModelLoader.loadAllModelsFromResources();
        detector = new HashLanguageDetector(models);
        Set<String> tmp = new HashSet<>();
        for (BaseLanguageModel m : models) {
            tmp.add(m.getLanguage().getISO3LetterName());
        }
        supportedLangs = Collections.unmodifiableSet(tmp);
    }

    @Override
    public Set<String> getSupportedLangs() {
        return supportedLangs;
    }

    @Override
    public List<LangDetectResult> detect(String s) {
        detector.reset();
        detector.addText(s);
        Collection<DetectionResult> results = detector.detect();
        List<LangDetectResult> ret = new ArrayList<>();
        for (DetectionResult r : results) {
            ret.add(new LangDetectResult(r.getLanguage().getISO3LetterName(), r.getScore()));
        }
        return ret;
    }
}
