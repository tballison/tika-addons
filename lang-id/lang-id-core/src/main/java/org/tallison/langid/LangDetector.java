package org.tallison.langid;

import java.util.List;
import java.util.Set;

public interface LangDetector {

    public Set<String> getSupportedLangs();
    public List<LangDetectResult> detect(String s);
}
