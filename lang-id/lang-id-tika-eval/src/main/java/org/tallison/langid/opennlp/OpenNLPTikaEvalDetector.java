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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import opennlp.tools.langdetect.Language;
import opennlp.tools.langdetect.LanguageDetector;
import opennlp.tools.langdetect.LanguageDetectorModel;
import opennlp.tools.util.normalizer.CharSequenceNormalizer;
import opennlp.tools.util.normalizer.EmojiCharSequenceNormalizer;
import opennlp.tools.util.normalizer.NumberCharSequenceNormalizer;
import opennlp.tools.util.normalizer.ShrinkCharSequenceNormalizer;
import opennlp.tools.util.normalizer.TwitterCharSequenceNormalizer;
import org.tallison.langid.LangDetectResult;
import org.tallison.langid.LangDetector;


public class OpenNLPTikaEvalDetector implements LangDetector {
    LanguageDetector detector;
    Set<String> supportedLangs;
    public OpenNLPTikaEvalDetector() throws IOException {
        try (InputStream is = this.getClass().getResourceAsStream("/model_20190626.bin")) {
            detector = new ProbingLanguageDetector(new LanguageDetectorModel(is), getNormalizers());
        }
        Set<String> tmp = new HashSet<>();
        for (String lang : detector.getSupportedLanguages()) {
            tmp.add(lang);
        }
        supportedLangs = Collections.unmodifiableSet(tmp);
        System.out.println("I support: " + supportedLangs.size());
    }

    @Override
    public Set<String> getSupportedLangs() {
        return supportedLangs;
    }

    @Override
    public List<LangDetectResult> detect(String s) {
        Language[] langs = detector.predictLanguages(s);
        List<LangDetectResult> results = new ArrayList<>();
        for (int i = 0; i < langs.length; i++) {
            results.add(new LangDetectResult(langs[i].getLang(), langs[i].getConfidence()));
        }
        return results;
    }

    private static CharSequenceNormalizer[] getNormalizers() {
        return new CharSequenceNormalizer[]{
                EmojiCharSequenceNormalizer.getInstance(),
                TikaUrlCharSequenceNormalizer.getInstance(),
                TwitterCharSequenceNormalizer.getInstance(),
                //AlphaOnlySequenceNormalizer.getInstance(),
                NumberCharSequenceNormalizer.getInstance(),
                ShrinkCharSequenceNormalizer.getInstance()
        };
    }

    private static class TikaUrlCharSequenceNormalizer implements CharSequenceNormalizer {
        //use this custom copy/paste of opennlo to avoid long, long hang with mail_regex
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

    private static class AlphaOnlySequenceNormalizer implements CharSequenceNormalizer {
        //use this custom copy/paste of opennlo to avoid long, long hang with mail_regex
        //TIKA-2777
        private static final Pattern REGEX = Pattern.compile("(\\p{IsAlphabetic}+)");
        private static final AlphaOnlySequenceNormalizer INSTANCE =
                new AlphaOnlySequenceNormalizer();

        public static AlphaOnlySequenceNormalizer getInstance() {
            return INSTANCE;
        }

        private AlphaOnlySequenceNormalizer() {
        }

        @Override
        public CharSequence normalize(CharSequence charSequence) {
            StringBuilder sb = new StringBuilder();
            Matcher m = REGEX.matcher(charSequence);
            while (m.find()) {
                sb.append(m.group(1)).append(" ");
            }
            return sb.toString();
        }
    }
}
