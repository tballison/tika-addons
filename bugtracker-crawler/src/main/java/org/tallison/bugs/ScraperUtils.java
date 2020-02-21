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
package org.tallison.bugs;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.tika.Tika;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.mime.MimeTypeException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;

class ScraperUtils {

    private static Tika TIKA = new Tika();
    private static TikaConfig TIKA_CONFIG = TikaConfig.getDefaultConfig();

    public static Instant getCreated(DateTimeFormatter formatter, String created) {

        try {
            TemporalAccessor t = formatter.parse(created);
            return Instant.from(t);
        } catch (Exception e) {
            System.err.println("trying to parse >"+created+"<");
            e.printStackTrace();
        }
        return Instant.now();
    }

    public static String getExtension(Path targ) {
        try (InputStream tis = TikaInputStream.get(targ)) {
            MediaType mt = TIKA.getDetector().detect(tis, new Metadata());
            try {
                String ext = TIKA_CONFIG.getMimeRepository().forName(
                        mt.toString()).getExtension();
                if (ext != null) {
                    return ext;
                }
            } catch (MimeTypeException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return ".bin";
    }

    public static MediaType getMediaType(Path targ) {
        try (InputStream tis = TikaInputStream.get(targ)) {
            return TIKA.getDetector().detect(tis, new Metadata());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return MediaType.OCTET_STREAM;
    }

    public static String getExtension(MediaType mt) {
        try {
            String ext = TIKA_CONFIG.getMimeRepository().forName(
                    mt.toString()).getExtension();
            if (ext != null) {
                return ext;
            }
        } catch (MimeTypeException e) {
            e.printStackTrace();
        }
        return ".bin";
    }

    public static Path getInitialTarget(Path root, Attachment a, String issueId, int i) {
        String ext = FilenameUtils.getExtension(a.fileName);
        ext = ext.toLowerCase(Locale.US);
        if (a.fileName.toLowerCase(Locale.US).endsWith(".tar.gz")) {
            ext = "tgz";
        }
        Path targ = root.resolve(issueId + "-" + i + "." + ext);
        System.out.println("writing " + a.attachmentUrl + " " + targ);
        return targ;
    }

    public static void grabAttachment(Path root, Attachment a, String issueId, int i) throws IOException {

        Path targ = getInitialTarget(root, a, issueId, i);

        if (Files.isRegularFile(targ)) {
            return;
        }
        byte[] raw = null;
        try {
            raw = HttpUtils.get(a.attachmentUrl);
        } catch (ClientException | IllegalArgumentException e) {
            System.err.println(a.attachmentUrl);
            e.printStackTrace();
            return;
        }
        writeAttachment(raw, targ, root, issueId, i, a.created);
    }

    public static void writeAttachment(byte[] raw, Path targ, Path root, String issueId,
                                        int i, Instant created) throws IOException {
        if (raw == null) {
            return;
        }
        try (OutputStream os = Files.newOutputStream(targ)) {
            IOUtils.copy(new ByteArrayInputStream(raw), os);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //if the extension was blank or longer than 5 letters, detect file type with Tika
        //then mv the file to the new name
        String ext = FilenameUtils.getExtension(targ.getFileName().toString());
        if (StringUtils.isBlank(ext) || ext.length() > 5 || ext.contains("-\\d")) {
            String detectedExtension = ScraperUtils.getExtension(targ);
            //detected extension includes "."
            System.out.println("detected ext " + targ + " " + detectedExtension);
            Path newTarg = root.resolve(issueId + "-" + i + detectedExtension);
            Files.move(targ, newTarg, StandardCopyOption.REPLACE_EXISTING);
            targ = newTarg;
        }
        Files.setLastModifiedTime(targ, FileTime.from(created));

    }

}
