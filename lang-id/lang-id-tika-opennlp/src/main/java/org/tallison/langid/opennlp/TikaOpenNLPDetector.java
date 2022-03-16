/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.tallison.langid.opennlp;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import opennlp.tools.langdetect.Language;
import opennlp.tools.langdetect.LanguageDetectorModel;
import opennlp.tools.util.normalizer.CharSequenceNormalizer;
import opennlp.tools.util.normalizer.EmojiCharSequenceNormalizer;
import opennlp.tools.util.normalizer.NumberCharSequenceNormalizer;
import opennlp.tools.util.normalizer.ShrinkCharSequenceNormalizer;
import opennlp.tools.util.normalizer.TwitterCharSequenceNormalizer;

import org.tallison.langid.LangDetectResult;
import org.tallison.langid.LangDetector;

/**
 * <p>
 * This is based on OpenNLP's language detector.  However,
 * we've built our own ProbingLanguageDetector and our own language
 * models from an extended Leipzig corpus.
 * </p>
 * <p>
 * Going forward, we plan to fold these improvements into OpenNLP
 * and remove our own custom code.
 * </p>
 */
public class TikaOpenNLPDetector implements LangDetector {

    static LanguageDetectorModel LANG_MODEL;

    static void loadBuiltInModels() throws IOException {
        try (InputStream is = TikaOpenNLPDetector.class.getResourceAsStream(
                "/model-20210401.bin"
        )) {
            LANG_MODEL = new LanguageDetectorModel(is);
        }
    }
    static {
        try {
            loadBuiltInModels();
        } catch (IOException e) {
            throw new RuntimeException("Can't find built-in language models");
        }
    }

    private static CharSequenceNormalizer[] getNormalizers() {
        return new CharSequenceNormalizer[]{
                TikaUrlCharSequenceNormalizer.getInstance(),
                AlphaIdeographSequenceNormalizer.getInstance(),
                EmojiCharSequenceNormalizer.getInstance(),
                TwitterCharSequenceNormalizer.getInstance(),
                NumberCharSequenceNormalizer.getInstance(),
                ShrinkCharSequenceNormalizer.getInstance()
        };
    }

    private final ProbingLanguageDetector detector = new ProbingLanguageDetector(LANG_MODEL, getNormalizers());
    private final StringBuilder buffer = new StringBuilder();

    public TikaOpenNLPDetector() {

    }


    public void setMaxLength(int maxLength) {
        detector.setMaxLength(maxLength);
    }

    public String[] getSupportedLanguages() {
        return detector.getSupportedLanguages();
    }


    @Override
    public Set<String> getSupportedLangs() {
        String[] langs = detector.getSupportedLanguages();
        Set<String> ret = new HashSet<>();
        for (String l : langs) {
            ret.add(l);
        }
        return ret;
    }

    @Override
    public List<LangDetectResult> detect(String s) {
        Language[] langs = detector.predictLanguages(s);
        List<LangDetectResult> ret = new ArrayList<>();
        for (Language lang : langs) {
            ret.add(new LangDetectResult(lang.getLang(), lang.getConfidence()));
        }
        return ret;
    }

    private static class TikaUrlCharSequenceNormalizer implements CharSequenceNormalizer {
        //use this custom copy/paste of opennlp to avoid long, long hang with mail_regex
        //TIKA-2777
        private static final Pattern URL_REGEX = Pattern.compile("https?://[-_.?&~;+=/#0-9A-Za-z]{10,10000}");
        private static final Pattern MAIL_REGEX = Pattern.compile("[-_.0-9A-Za-z]{1,100}@[-_0-9A-Za-z]{1,100}[-_.0-9A-Za-z]{1,100}");
        private static final TikaUrlCharSequenceNormalizer INSTANCE = new TikaUrlCharSequenceNormalizer();

        public static TikaUrlCharSequenceNormalizer getInstance() {
            return INSTANCE;
        }

        private TikaUrlCharSequenceNormalizer() {
        }

        @Override
        public CharSequence normalize(CharSequence charSequence) {
            String modified = URL_REGEX.matcher(charSequence).replaceAll(" ");
            return MAIL_REGEX.matcher(modified).replaceAll(" ");
        }
    }

    private static class AlphaIdeographSequenceNormalizer implements CharSequenceNormalizer {
        private static final Pattern REGEX = Pattern.compile("[^\\p{IsAlphabetic}\\p{IsIdeographic}]+");
        private static final AlphaIdeographSequenceNormalizer INSTANCE = new AlphaIdeographSequenceNormalizer();

        public static AlphaIdeographSequenceNormalizer getInstance() {
            return INSTANCE;
        }

        private AlphaIdeographSequenceNormalizer() {
        }

        @Override
        public CharSequence normalize(CharSequence charSequence) {
            return REGEX.matcher(charSequence).replaceAll(" ");
        }
    }
}
