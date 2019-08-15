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
package org.tallison.tikaeval.example;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.solr.common.SolrInputDocument;
import org.apache.tika.eval.langid.Language;
import org.apache.tika.eval.langid.LanguageIDWrapper;
import org.apache.tika.eval.textstats.BasicTokenCountStatsCalculator;
import org.apache.tika.eval.textstats.CommonTokens;
import org.apache.tika.eval.textstats.CompositeTextStatsCalculator;
import org.apache.tika.eval.textstats.ContentLengthCalculator;
import org.apache.tika.eval.textstats.TextStatsCalculator;
import org.apache.tika.eval.textstats.TokenEntropy;
import org.apache.tika.eval.tokens.CommonTokenResult;
import org.apache.tika.eval.tokens.TokenCounts;
import org.apache.tika.eval.util.EvalExceptionUtils;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.PDF;
import org.apache.tika.metadata.PagedText;
import org.apache.tika.metadata.Property;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.sax.RecursiveParserWrapperHandler;

public class TikaEvalDocMapper implements DocMapper {

    private static final int TRUNCATED_LENGTH = 1000;

    private final CompositeTextStatsCalculator textStatsCalculator;

    public TikaEvalDocMapper() {
        textStatsCalculator = _initCalculator();
    }

    private CompositeTextStatsCalculator _initCalculator() {
        List<TextStatsCalculator> calculators = new ArrayList<>();
        calculators.add(new CommonTokens());
        calculators.add(new BasicTokenCountStatsCalculator());
        calculators.add(new ContentLengthCalculator());
        calculators.add(new TokenEntropy());
        return new CompositeTextStatsCalculator(calculators);
    }

    @Override
    public SolrInputDocument map(Metadata metadata) {
        SolrInputDocument doc = new SolrInputDocument();
        addContent(doc, metadata);
        tryToAddDate(TikaCoreProperties.CREATED, "created", metadata, doc);
        tryToAddDate(TikaCoreProperties.MODIFIED, "modified", metadata, doc);
        tryToAddString(TikaCoreProperties.CREATOR, "authors", metadata, doc);
        tryToAddString(Metadata.CONTENT_TYPE, "mime", metadata, doc);
        handleStackTrace(getStackTrace(metadata), doc);
        return doc;
    }

    private void handleStackTrace(String stackTrace, SolrInputDocument doc) {
        if (StringUtils.isBlank(stackTrace)) {
            return;
        }

        String stackTraceFacet = EvalExceptionUtils.normalize(stackTrace);
        if (!StringUtils.isBlank(stackTraceFacet)) {
            doc.setField("stacktrace_facet", stackTraceFacet);
        }
    }

    private String getStackTrace(Metadata metadata) {
        String stackTrace = metadata.get(RecursiveParserWrapperHandler.EMBEDDED_EXCEPTION);
        if (stackTrace != null) {
            return stackTrace;
        }
        return metadata.get(TikaCoreProperties.TIKA_META_EXCEPTION_PREFIX+"runtime");
    }

    private void tryToAddString(Object property, String fieldName, Metadata metadata,
                                SolrInputDocument doc) {
        if (property instanceof Property && ((Property)property).isMultiValuePermitted()) {
            List<String> values = new ArrayList<>();
            for (String v : metadata.getValues((Property)property)) {
                if (!StringUtils.isBlank(v)) {
                    values.add(v);
                }
            }
            doc.setField(fieldName, values);
        } else {
            String value = null;

            if (property instanceof Property) {
                value = metadata.get((Property)property);
            } else {
                value = metadata.get((String)property);
            }
            if (!StringUtils.isBlank(value)) {
                doc.setField(fieldName, value);
            }
        }
    }

    private void tryToAddDate(Property property, String fieldName, Metadata metadata,
                              SolrInputDocument doc) {
        if (metadata.getDate(property) != null) {
            doc.setField(fieldName, metadata.getDate(property));
        }
    }

    private void addContent(SolrInputDocument doc, Metadata metadata) {
        String content = metadata.get(RecursiveParserWrapperHandler.TIKA_CONTENT);
        if (StringUtils.isBlank(content)) {
            return;
        }
        content = content.replaceAll("\n+", "\n");
        content = content.trim();
        String truncated = content;
        if (content.length() > TRUNCATED_LENGTH) {
            truncated = truncated.substring(0,TRUNCATED_LENGTH);
        }
        doc.setField("content_trunc", truncated);
        doc.setField("content", content);

        //now add tika-eval stats
        Map<Class, Object> stats = textStatsCalculator.calculate(content);
        CommonTokenResult commonTokenResult = (CommonTokenResult)stats.get(CommonTokens.class);
        doc.setField("oov", commonTokenResult.getOOV());
        doc.setField("num_alpha_tokens", commonTokenResult.getAlphabeticTokens());
        doc.setField("num_common_tokens", commonTokenResult.getCommonTokens());
        TokenCounts tokenCounts = (TokenCounts) stats.get(BasicTokenCountStatsCalculator.class);

        doc.setField("num_tokens", tokenCounts.getTotalTokens());

        Double tokenEntropy = (Double)stats.get(TokenEntropy.class);
        if (tokenEntropy != null && ! tokenEntropy.isNaN()) {
            doc.setField("token_entropy", tokenEntropy);
        }
        List<Language> detectedLanguages = (List<Language>) stats.get(LanguageIDWrapper.class);
        if (detectedLanguages != null && detectedLanguages.size() > 0) {
            doc.setField("lang", detectedLanguages.get(0).getLanguage());
            doc.setField("lang_conf", detectedLanguages.get(0).getConfidence());
        }

        if (metadata.get(PagedText.N_PAGES) != null) {
            int numPages = metadata.getInt(PagedText.N_PAGES);
            if (numPages > 0) {
                float tokensPerPage =
                        (float)tokenCounts.getTotalTokens()/(float)numPages;
                doc.setField("tokens_per_page", tokensPerPage);
            }
        }

        if (metadata.get(PDF.CHARACTERS_PER_PAGE) != null) {
            SummaryStatistics summaryStatistics = new SummaryStatistics();
            int[] chars = metadata.getIntValues(PDF.CHARACTERS_PER_PAGE);
            int[] unmapped = metadata.getIntValues(PDF.UNMAPPED_UNICODE_CHARS_PER_PAGE);
            if (chars != null && unmapped != null) {
                for (int i = 0; i < chars.length && i < unmapped.length; i++) {
                    if (chars[i] > 0) {
                        summaryStatistics.addValue((double) unmapped[i] / (double) chars[i]);
                    }
                }
                doc.setField("pdf_percent_unicode_mapped", summaryStatistics.getMean());
            }
        }
    }

}
