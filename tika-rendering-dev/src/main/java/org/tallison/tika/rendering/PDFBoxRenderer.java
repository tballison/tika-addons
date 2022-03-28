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
package org.tallison.tika.rendering;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.commons.io.IOExceptionWithCause;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.ImageType;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.tools.imageio.ImageIOUtil;

import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TemporaryResources;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Property;
import org.apache.tika.parser.ParseContext;

public class PDFBoxRenderer implements Renderer {
    public static Property PAGE_NUMBER = Property.externalInteger("renderer:page_number");

    @Override
    public RenderResults render(InputStream is, ParseContext parseContext) throws IOException, TikaException {
        TikaInputStream tis = TikaInputStream.get(is);
        RenderResults results = new RenderResults(new TemporaryResources());
        try (PDDocument pdDocument = PDDocument.load(tis.getPath().toFile())) {

            PDFRenderer renderer = new PDFRenderer(pdDocument);

            for (int i = 0; i < pdDocument.getNumberOfPages(); i++) {
                try {
                    Metadata m = new Metadata();
                    m.set(PAGE_NUMBER, i + 1);
                    Path imagePath = renderPage(renderer, i);
                    results.add(new RenderResult(imagePath, m));
                } catch (IOException e) {
                    //do something useful
                }
            }
        }
        return results;
    }

    private Path renderPage(PDFRenderer renderer, int pageIndex)
            throws IOException {

        int dpi = 300;
        Path tmpFile = Files.createTempFile("tika-rendering-", "-" + (pageIndex + 1) + ".png");
        try {
            long start = System.currentTimeMillis();
            BufferedImage image = renderer.renderImageWithDPI(pageIndex, dpi, ImageType.GRAY);
            long elapsed = System.currentTimeMillis() - start;
            start = System.currentTimeMillis();
            try (OutputStream os = Files.newOutputStream(tmpFile)) {
                ImageIOUtil.writeImage(image, "tiff", os, dpi);
            }
            long elapsedWrite = System.currentTimeMillis()-start;
            //System.out.println("elapsed to render: " + elapsed + " elapsed to write " +
            // elapsedWrite);
        } catch (SecurityException e) {
            //throw SecurityExceptions immediately
            throw e;
        } catch (IOException | RuntimeException e) {
            throw new IOExceptionWithCause(e);
        }
        return tmpFile;
    }

}
