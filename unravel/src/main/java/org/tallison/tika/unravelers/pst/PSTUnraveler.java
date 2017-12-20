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
package org.tallison.tika.unravelers.pst;

import com.pff.PSTAttachment;
import com.pff.PSTException;
import com.pff.PSTFile;
import com.pff.PSTFolder;
import com.pff.PSTMessage;
import com.pff.PSTRecipient;
import org.apache.tika.exception.TikaException;
import org.apache.tika.extractor.EmbeddedDocumentUtil;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Message;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Office;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.RecursiveParserWrapper;
import org.apache.tika.parser.mbox.OutlookPSTParser;
import org.apache.tika.parser.microsoft.OutlookExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.tika.unravelers.MyRecursiveParserWrapper;
import org.tallison.tika.unravelers.PostParseHandler;
import org.tallison.tika.unravelers.RecursiveRoot;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;
import org.xml.sax.helpers.DefaultHandler;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static java.lang.String.valueOf;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singleton;

/**
 * WARNING: THIS IS NOT THREAD SAFE!!!
 */
public class PSTUnraveler extends AbstractParser {

    private static final Logger LOG = LoggerFactory.getLogger(PSTUnraveler.class);

    private static final Set<MediaType> SUPPORTED_TYPES = singleton(
            OutlookPSTParser.MS_OUTLOOK_PST_MIMETYPE);

    private static final ContentHandler DEFAULT_HANDLER = new DefaultHandler();

    private static AttributesImpl createAttribute(String attName, String attValue) {
        AttributesImpl attributes = new AttributesImpl();
        attributes.addAttribute("", attName, attName, "CDATA", attValue);
        return attributes;
    }

    private final PostParseHandler postParseHandler;
    private final MyRecursiveParserWrapper recursiveParserWrapper;

    public PSTUnraveler(PostParseHandler postParseHandler, MyRecursiveParserWrapper recursiveParserWrapper) {
        this.postParseHandler = postParseHandler;
        this.recursiveParserWrapper = recursiveParserWrapper;
    }

    @Override
    public Set<MediaType> getSupportedTypes(ParseContext context) {
        return SUPPORTED_TYPES;
    }

    public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context)
            throws IOException, SAXException, TikaException {


        TikaInputStream in = TikaInputStream.get(stream);
        PSTFile pstFile = null;
        try {
            pstFile = new PSTFile(in.getFile().getPath());
            boolean isValid = pstFile.getFileHandle().getFD().valid();
            metadata.set("isValid", valueOf(isValid));
            if (isValid) {
                parseFolder(pstFile.getRootFolder());
            }
        } catch (Exception e) {
            throw new TikaException(e.getMessage(), e);
        } finally {
            if (pstFile != null && pstFile.getFileHandle() != null) {
                try {
                    pstFile.getFileHandle().close();
                } catch (IOException e) {
                    //swallow closing exception
                }
            }
        }

    }

    private void parseFolder(PSTFolder pstFolder)
            throws Exception {
        if (pstFolder.getContentCount() > 0) {
            PSTMessage pstMail = (PSTMessage) pstFolder.getNextChild();
            while (pstMail != null) {

                final Metadata mailMetadata = new Metadata();
                //parse attachments first so that stream exceptions
                //in attachments can make it into mailMetadata.
                //RecursiveParserWrapper copies the metadata and thereby prevents
                //modifications to mailMetadata from making it into the
                //metadata objects cached by the RecursiveParserWrapper
                try {
                    parseMailAttachments(pstMail, mailMetadata);
                    parserMailItem(pstMail, mailMetadata);
                    postParse(mailMetadata);
                } finally {
                    recursiveParserWrapper.reset();
                }

                pstMail = (PSTMessage) pstFolder.getNextChild();
            }
        }

        if (pstFolder.hasSubfolders()) {
            for (PSTFolder pstSubFolder : pstFolder.getSubFolders()) {
                parseFolder(pstSubFolder);
            }
        }
    }

    private void postParse(Metadata mailMetadata) {
        List<Metadata> metadataList = new ArrayList<>();
        metadataList.add(mailMetadata);
        metadataList.addAll(recursiveParserWrapper.getMetadata());
        try {
            postParseHandler.handle(metadataList);
        } catch (IOException|TikaException e) {
            LOG.warn("problem writing output", e);
        }
    }

    private void parserMailItem(PSTMessage pstMail, Metadata mailMetadata) throws SAXException, IOException {
        mailMetadata.set(Metadata.RESOURCE_NAME_KEY, pstMail.getInternetMessageId());
        mailMetadata.set(Metadata.EMBEDDED_RELATIONSHIP_ID, pstMail.getInternetMessageId());
        mailMetadata.set(TikaCoreProperties.IDENTIFIER, pstMail.getInternetMessageId());
        mailMetadata.set(TikaCoreProperties.TITLE, pstMail.getSubject());
        mailMetadata.set(Metadata.MESSAGE_FROM, pstMail.getSenderName());
        mailMetadata.set(TikaCoreProperties.CREATOR, pstMail.getSenderName());
        mailMetadata.set(TikaCoreProperties.CREATED, pstMail.getCreationTime());
        mailMetadata.set(TikaCoreProperties.MODIFIED, pstMail.getLastModificationTime());
        mailMetadata.set(TikaCoreProperties.COMMENTS, pstMail.getComment());
        mailMetadata.set("descriptorNodeId", valueOf(pstMail.getDescriptorNodeId()));
        mailMetadata.set("senderEmailAddress", pstMail.getSenderEmailAddress());
        mailMetadata.set("recipients", pstMail.getRecipientsString());
        mailMetadata.set("displayTo", pstMail.getDisplayTo());
        mailMetadata.set("displayCC", pstMail.getDisplayCC());
        mailMetadata.set("displayBCC", pstMail.getDisplayBCC());
        mailMetadata.set("importance", valueOf(pstMail.getImportance()));
        mailMetadata.set("priority", valueOf(pstMail.getPriority()));
        mailMetadata.set("flagged", valueOf(pstMail.isFlagged()));
        mailMetadata.set(Office.MAPI_MESSAGE_CLASS,
                OutlookExtractor.getMessageClass(pstMail.getMessageClass()));

        mailMetadata.set(Message.MESSAGE_FROM_EMAIL, pstMail.getSenderEmailAddress());

        mailMetadata.set(Office.MAPI_FROM_REPRESENTING_EMAIL,
                pstMail.getSentRepresentingEmailAddress());

        mailMetadata.set(Message.MESSAGE_FROM_NAME, pstMail.getSenderName());
        mailMetadata.set(Office.MAPI_FROM_REPRESENTING_NAME, pstMail.getSentRepresentingName());

        //add recipient details
        try {
            for (int i = 0; i < pstMail.getNumberOfRecipients(); i++) {
                PSTRecipient recipient = pstMail.getRecipient(i);
                switch (OutlookExtractor.RECIPIENT_TYPE.getTypeFromVal(recipient.getRecipientType())) {
                    case TO:
                        OutlookExtractor.addEvenIfNull(Message.MESSAGE_TO_DISPLAY_NAME,
                                recipient.getDisplayName(), mailMetadata);
                        OutlookExtractor.addEvenIfNull(Message.MESSAGE_TO_EMAIL,
                                recipient.getEmailAddress(), mailMetadata);
                        break;
                    case CC:
                        OutlookExtractor.addEvenIfNull(Message.MESSAGE_CC_DISPLAY_NAME,
                                recipient.getDisplayName(), mailMetadata);
                        OutlookExtractor.addEvenIfNull(Message.MESSAGE_CC_EMAIL,
                                recipient.getEmailAddress(), mailMetadata);
                        break;
                    case BCC:
                        OutlookExtractor.addEvenIfNull(Message.MESSAGE_BCC_DISPLAY_NAME,
                                recipient.getDisplayName(), mailMetadata);
                        OutlookExtractor.addEvenIfNull(Message.MESSAGE_BCC_EMAIL,
                                recipient.getEmailAddress(), mailMetadata);
                        break;
                    default:
                        //do we want to handle unspecified or unknown?
                        break;
                }
            }
        } catch (PSTException e) {
            LOG.warn("pst problem", e);
        }
        //we may want to experiment with working with the bodyHTML.
        //However, because we can't get the raw bytes, we _could_ wind up sending
        //a UTF-8 byte representation of the html that has a conflicting metaheader
        //that causes the HTMLParser to get the encoding wrong.  Better if we could get
        //the underlying bytes from the pstMail object...

        byte[] mailContent = pstMail.getBody().getBytes(UTF_8);
        mailMetadata.set(TikaCoreProperties.CONTENT_TYPE_OVERRIDE, MediaType.TEXT_PLAIN.toString());
        mailMetadata.set(RecursiveParserWrapper.TIKA_CONTENT, pstMail.getBody());
    }

    private void parseMailAttachments(PSTMessage email,
                                      final Metadata mailMetadata)
            throws TikaException {
        int numberOfAttachments = email.getNumberOfAttachments();

        for (int i = 0; i < numberOfAttachments; i++) {
            try {
                PSTAttachment attach = email.getAttachment(i);

                // Get the filename; both long and short filenames can be used for attachments
                String filename = attach.getLongFilename();
                if (filename.isEmpty()) {
                    filename = attach.getFilename();
                }

                Metadata attachMeta = new Metadata();
                attachMeta.set(Metadata.RESOURCE_NAME_KEY, filename);
                attachMeta.set(Metadata.EMBEDDED_RELATIONSHIP_ID, filename);
                ParseContext context = new ParseContext();
                context.set(RecursiveRoot.class, new RecursiveRoot("/"+filename));
                    TikaInputStream tis = null;
                    try {
                        tis = TikaInputStream.get(attach.getFileInputStream());
                    } catch (NullPointerException e) {//TIKA-2488
                        EmbeddedDocumentUtil.recordEmbeddedStreamException(e, mailMetadata);
                        continue;
                    }

                    try {
                        recursiveParserWrapper.parse(tis, DEFAULT_HANDLER, attachMeta, context);
                    } catch (Exception e) {
                        EmbeddedDocumentUtil.recordException(e, attachMeta);
                    } finally {
                        tis.close();
                    }

            } catch (Exception e) {
                throw new TikaException("Unable to unpack document stream", e);
            }
        }
    }
}
