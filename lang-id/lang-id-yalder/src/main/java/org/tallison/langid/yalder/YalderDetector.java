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
package org.tallison.langid.yalder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.krugler.yalder.BaseLanguageModel;
import org.krugler.yalder.DetectionResult;
import org.krugler.yalder.ModelLoader;
import org.krugler.yalder.hash.HashLanguageDetector;
import org.tallison.langid.LangDetectResult;
import org.tallison.langid.LangDetector;

public class YalderDetector implements LangDetector {

    static Map<String, String> LANG_MAPPINGS = new ConcurrentHashMap<>();
    static {
        String[] mappings = new String[]{
                "zho", "cmn"
        };
        for (int i = 0; i < mappings.length-1; i += 1) {
            LANG_MAPPINGS.put(mappings[i], mappings[i+1]);
        }
    }

    Collection<BaseLanguageModel> models;
    HashLanguageDetector detector;
    Set<String> supportedLangs;



    public YalderDetector() throws IOException {
        models = ModelLoader.loadAllModelsFromResources();
        detector = new HashLanguageDetector(models);
        Set<String> tmp = new HashSet<>();
        for (BaseLanguageModel m : models) {
            String l = m.getLanguage().getISO3LetterName();
            if (LANG_MAPPINGS.containsKey(l)) {
                l = LANG_MAPPINGS.get(l);
            }
            tmp.add(l);
        }
        supportedLangs = Collections.unmodifiableSet(tmp);
    }

    public void stopEarly(boolean b) {
        detector.stopEarly(b);
    }

    public int getTokensPerDetect() {
        return detector.getTokensPerDetect();
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
            String l = r.getLanguage().getISO3LetterName();
            if (LANG_MAPPINGS.containsKey(l)) {
                l = LANG_MAPPINGS.get(l);
            }
            ret.add(new LangDetectResult(l, r.getScore()));
        }
        return ret;
    }
}
