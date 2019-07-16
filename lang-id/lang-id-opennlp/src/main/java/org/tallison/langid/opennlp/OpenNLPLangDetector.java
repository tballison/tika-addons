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

import opennlp.tools.langdetect.Language;
import opennlp.tools.langdetect.LanguageDetector;
import opennlp.tools.langdetect.LanguageDetectorME;
import opennlp.tools.langdetect.LanguageDetectorModel;
import org.tallison.langid.LangDetectResult;
import org.tallison.langid.LangDetector;


public class OpenNLPLangDetector implements LangDetector {
    LanguageDetector detector;
    Set<String> supportedLangs;
    public OpenNLPLangDetector() throws IOException {
        try (InputStream is = this.getClass().getResourceAsStream("/langdetect-183.bin")) {
            detector = new LanguageDetectorME(new LanguageDetectorModel(is));
        }
        Set<String> tmp = new HashSet<>();
        for (String lang : detector.getSupportedLanguages()) {
            tmp.add(lang);
        }
        supportedLangs = Collections.unmodifiableSet(tmp);
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
}
