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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.tika.eval.langid.Language;
import org.apache.tika.eval.langid.LanguageIDWrapper;
import org.apache.tika.eval.textstats.BasicTokenCountStatsCalculator;
import org.apache.tika.eval.textstats.CommonTokens;
import org.apache.tika.eval.textstats.CompositeTextStatsCalculator;
import org.apache.tika.eval.textstats.ContentLengthCalculator;
import org.apache.tika.eval.textstats.TextProfileSignature;
import org.apache.tika.eval.textstats.TextSha256Signature;
import org.apache.tika.eval.textstats.TextStatsCalculator;
import org.apache.tika.eval.textstats.TokenEntropy;
import org.apache.tika.eval.tokens.CommonTokenResult;
import org.apache.tika.eval.tokens.TokenCounts;
import org.apache.tika.eval.util.EvalExceptionUtils;
import org.apache.tika.metadata.DublinCore;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.PDF;
import org.apache.tika.metadata.PagedText;
import org.apache.tika.metadata.Property;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.sax.RecursiveParserWrapperHandler;

public class TikaEvalDocMapper implements DocMapper {

    private static final int TRUNCATED_LENGTH = 1000;
    private static final Property CONTAINER_CREATED = Property.internalDate("container_created");
    private static final Property[] BOOLEAN_PROPERTIES =
            new Property[]{
                    PDF.HAS_ACROFORM_FIELDS,
                    PDF.HAS_XFA, PDF.HAS_XMP, TikaCoreProperties.HAS_SIGNATURE,
            };
    private static Map<Property, String> TAGS = new HashMap<>();

    static {
        TAGS.put(PDF.HAS_ACROFORM_FIELDS, "acro_fields");
        TAGS.put(PDF.HAS_XFA, "xfa");
        TAGS.put(PDF.HAS_XMP, "xmp");
        TAGS.put(TikaCoreProperties.HAS_SIGNATURE, "digital_signature");
    }
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
        calculators.add(new TextSha256Signature());
        calculators.add(new TextProfileSignature());
        return new CompositeTextStatsCalculator(calculators);
    }

    @Override
    public List<Metadata> map(List<Metadata> metadataList) {
        int totalEmbedded = metadataList.size()-1;
        long inlineAttachments = metadataList.stream().filter( m ->
                TikaCoreProperties.EmbeddedResourceType.INLINE.toString()
                        .equals(m.get(TikaCoreProperties.EMBEDDED_RESOURCE_TYPE))).count();

        long macros = metadataList.stream().filter( m ->
                TikaCoreProperties.EmbeddedResourceType.MACRO.toString()
                        .equals(m.get(TikaCoreProperties.EMBEDDED_RESOURCE_TYPE))).count();
        long numAttachments = totalEmbedded-inlineAttachments-macros;

        List<Metadata> ret = new ArrayList<>();
        Date containerCreated = metadataList.get(0).getDate(TikaCoreProperties.CREATED);
        for (int i = 0; i < metadataList.size(); i++) {
            Metadata mapped = map(metadataList.get(i));
            if (i == 0) {
                mapped.set("is_embedded", "false");
                mapped.set("total_embedded", Integer.toString(totalEmbedded));
                mapped.set("num_attachments", Long.toString(numAttachments));
                mapped.set("num_inline_attachments", Long.toString(inlineAttachments));
                if (metadataList.get(0).get(PagedText.N_PAGES) != null) {
                    Integer numPages = metadataList.get(0).getInt(PagedText.N_PAGES);
                    if (numPages != null && numPages > 0) {
                        double numInlineAttachmentsPerPage =
                                (double)inlineAttachments/(double)numPages;
                        mapped.set("inline_attachments_per_page", Double.toString(numInlineAttachmentsPerPage));
                    }
                }
                mapped.set("num_macros", Long.toString(macros));
            } else {
                mapped.set("is_embedded", "true");
                String type = metadataList.get(i).get(TikaCoreProperties.EMBEDDED_RESOURCE_TYPE);
                type = type == null ? "attachment" : type;
                mapped.set("embedded_type", type);
            }
            if (containerCreated != null) {
                mapped.set(CONTAINER_CREATED, containerCreated);
            }
            ret.add(mapped);
        }
        return ret;
    }


    private Metadata map(Metadata metadata) {
        Metadata doc = new Metadata();
        addContent(doc, metadata);
        if (metadata.get(TikaCoreProperties.HAS_SIGNATURE) != null &&
            "true".equalsIgnoreCase(metadata.get(TikaCoreProperties.HAS_SIGNATURE))) {
            doc.add("signature", "true");
        } else {
            doc.add("signature", "false");
        }

        tryToAddString(PDF.PDF_VERSION, "pdf_version", metadata, doc);
        tryToAddString(PDF.PDFA_VERSION, "pdfa_version", metadata, doc);
        tryToAddString(PDF.PDF_EXTENSION_VERSION, "pdf_extension_version", metadata, doc);
        tryToAddString(PDF.PDF_VERSION, "pdf_version", metadata, doc);
        tryToAddString(PDF.ACTION_TRIGGER, "action_trigger", metadata, doc);
        tryToAddString(TikaCoreProperties.FORMAT, "format", metadata, doc);
        tryToAddString(TikaCoreProperties.LANGUAGE, "lang", metadata, doc);

        tryToAddString("X-TIKA:digest:MD5", "md5", metadata, doc);
        tryToAddString("X-TIKA:digest:SHA256", "sha256", metadata, doc);
        tryToAddDate(TikaCoreProperties.CREATED, "created", metadata, doc);
        tryToAddDate(TikaCoreProperties.MODIFIED, "modified", metadata, doc);
        tryToAddString(TikaCoreProperties.CREATOR, "authors", metadata, doc);
        String mimeDetailed = metadata.get(Metadata.CONTENT_TYPE);
        String mime = mimeDetailed;
        if (mimeDetailed != null) {
            int i = mimeDetailed.indexOf(";");
            if (i > -1) {
                mime = mimeDetailed.substring(0, i);
                doc.add("mime", mime);
            } else {
                doc.add("mime", mimeDetailed);
            }
        }
        tryToAddString(Metadata.CONTENT_TYPE, "mime_detailed", metadata, doc);
        tryToAddString(TikaCoreProperties.TITLE, "title", metadata, doc);
        tryToAddString(DublinCore.SUBJECT, "subject", metadata, doc);
        tryToAddString(TikaCoreProperties.CREATOR_TOOL, "creator_tool", metadata, doc);
        tryToAddString(RecursiveParserWrapperHandler.EMBEDDED_DEPTH, "embedded_depth", metadata, doc);
        if (metadata.get(Metadata.CONTENT_LENGTH) != null) {
            tryToAddString(Metadata.CONTENT_LENGTH, "length", metadata, doc);
        }
        tryToAddString(RecursiveParserWrapperHandler.EMBEDDED_RESOURCE_PATH,
                "embedded_path", metadata, doc);
        handleStackTrace(getStackTrace(metadata), doc);
        addTags(metadata, doc);
        return doc;
    }

    private void addTags(Metadata metadata, Metadata doc) {
        int added = 0;
        StringBuilder sb = new StringBuilder();
        for (Property p : BOOLEAN_PROPERTIES) {
            String v = metadata.get(p);
            if (v != null && v.toLowerCase(Locale.US).equals("true")) {
                if (added > 0) {
                    sb.append(" ");
                }
                sb.append(TAGS.get(p));
                added++;
            }
        }
        String tags = sb.toString();
        if (! StringUtils.isAllBlank(tags)) {
            doc.set("tags", tags);
        }
    }

    private void tryToAddBoolean(Property property, String fieldName,
                                 Metadata metadata, Metadata doc) {
        if (metadata.get(property) != null &&
                metadata.get(property).equalsIgnoreCase("true")) {
            doc.set(fieldName, "true");
        } else {
            doc.set(fieldName, "false");
        }
    }

    private void handleStackTrace(String stackTrace, Metadata doc) {
        if (StringUtils.isBlank(stackTrace)) {
            return;
        }

        doc.set("stacktrace", stackTrace);
        String stackTraceFacet = EvalExceptionUtils.normalize(stackTrace);
        if (!StringUtils.isBlank(stackTraceFacet)) {
            doc.set("stacktrace_facet", stackTraceFacet);
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
                                Metadata doc) {

        if (property instanceof Property && ((Property)property).isMultiValuePermitted()) {
            for (String v : metadata.getValues((Property)property)) {
                doc.add(fieldName, v);
            }
        } else {
            String value = null;

            if (property instanceof Property) {
                value = metadata.get((Property)property);
            } else {
                value = metadata.get((String)property);
            }
            if (!StringUtils.isBlank(value)) {
                doc.set(fieldName, value);
            }
        }
    }

    private void tryToAddDate(Property property, String fieldName, Metadata metadata,
                              Metadata doc) {
        if (metadata.getDate(property) != null) {
            doc.set(fieldName, metadata.get(property));
        }
    }

    private void addContent(Metadata doc, Metadata metadata) {
        String content = metadata.get(RecursiveParserWrapperHandler.TIKA_CONTENT);
        if (StringUtils.isBlank(content)) {
            doc.set("missing_content", "true");
            doc.set("num_tokens", "0");
            return;
        }
        content = content.replaceAll("\n+", "\n");
        content = content.trim();
        String truncated = content;
        if (content.length() > TRUNCATED_LENGTH) {
            truncated = truncated.substring(0,TRUNCATED_LENGTH);
        }
        doc.set("content_trunc", truncated);
        doc.set("content", content);

        //now add tika-eval stats
        Map<Class, Object> stats = textStatsCalculator.calculate(content);
        CommonTokenResult commonTokenResult = (CommonTokenResult)stats.get(CommonTokens.class);

        int numAlpha = commonTokenResult.getAlphabeticTokens();
        if (numAlpha > 0) {
            doc.set("oov",
                    Double.toString(commonTokenResult.getOOV()));
        }
        doc.set("num_alpha_tokens",
                Integer.toString(commonTokenResult.getAlphabeticTokens()));
        doc.set("num_common_tokens",
                Integer.toString(commonTokenResult.getCommonTokens()));
        TokenCounts tokenCounts = (TokenCounts) stats.get(BasicTokenCountStatsCalculator.class);

        doc.set("num_tokens", Integer.toString(tokenCounts.getTotalTokens()));

        Double tokenEntropy = (Double)stats.get(TokenEntropy.class);
        if (tokenEntropy != null && ! tokenEntropy.isNaN()) {
            doc.set("token_entropy", Double.toString(tokenEntropy));
        }
        List<Language> detectedLanguages = (List<Language>) stats.get(LanguageIDWrapper.class);
        if (detectedLanguages != null && detectedLanguages.size() > 0) {
            doc.set("lang_detected", detectedLanguages.get(0).getLanguage());
            doc.set("lang_detected_conf", Double.toString(detectedLanguages.get(0).getConfidence()));
        }

        if (metadata.get(PagedText.N_PAGES) != null) {
            int numPages = metadata.getInt(PagedText.N_PAGES);
            if (numPages > 0) {
                float tokensPerPage =
                        (float)tokenCounts.getTotalTokens()/(float)numPages;
                doc.set("tokens_per_page", Double.toString(tokensPerPage));
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
                double avg = summaryStatistics.getMean();
                if (Double.isNaN(avg)) {
                    avg = 0.0;
                }
                doc.set("pdf_percent_unicode_not_mapped", Double.toString(avg));
            }
        }

        String textDigest = (String)stats.get(TextSha256Signature.class);
        doc.add("text_digest", textDigest);
        String textProfile = (String)stats.get(TextProfileSignature.class);
        doc.add("text_profile_digest", textProfile);
    }

}
