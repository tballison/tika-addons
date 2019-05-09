package org.tallison.langid;

public class LangDetectResult {

    private final String lang;
    private final double confidence;

    public LangDetectResult(String lang, double confidence) {
        this.lang = lang;
        this.confidence = confidence;
    }

    public String getLanguage() {
        return lang;
    }

    public double getConfidence() {
        return confidence;
    }
}
