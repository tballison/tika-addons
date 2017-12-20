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

package org.tallison.tika.unravelers;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.BasicContentHandlerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tallison.tika.unravelers.pst.PSTUnraveler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class UnravelCLI {

    private static final Logger LOG = LoggerFactory.getLogger(UnravelCLI.class);


    private static Options OPTIONS;

    static {
        OPTIONS = new Options();
        OPTIONS.addOption("i", "input", true, "input container file or directory of container files")
                .addOption("o", "output", true, "output directory");

    }


    public static void main(String[] args) throws Exception {
        DefaultParser defaultCLIParser = new DefaultParser();
        CommandLine commandLine = null;
        try {
            commandLine = defaultCLIParser.parse(OPTIONS, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            USAGE();
            return;
        }

        Path inputFile = null;
        Path outputDir = null;

        if (commandLine.hasOption("i")) {
            inputFile = Paths.get(commandLine.getOptionValue("i"));
        }
        if (inputFile == null) {
            System.err.println("must specify an input file -i");
            USAGE();
            return;
        } else if (! Files.isDirectory(inputFile) && ! Files.isRegularFile(inputFile)) {
            System.err.println("-i must point to a container file or a directory");
            USAGE();
            return;
        }
        if (commandLine.hasOption("o")) {
            outputDir = Paths.get(commandLine.getOptionValue("o"));
        }
        if (Files.isRegularFile(outputDir)) {
            System.err.println("must specify a directory with -o, not a file: "+outputDir);
        }
        if (! Files.isDirectory(inputFile)) {
            handleFile(inputFile, outputDir, null);
        } else {
            for (File f : inputFile.toFile().listFiles()) {
                int i = f.getName().lastIndexOf(".");
                String fnameToDir = f.getName();
                if (i > -1) {
                    fnameToDir = fnameToDir.substring(0, i);
                }
                handleFile(f.toPath(), outputDir, fnameToDir);
            }
        }
    }

    private static void handleFile(Path inputFile, Path root, String subDir) throws IOException, TikaException, SAXException {
        PSTUnraveler pstUnraveler = new PSTUnraveler(new DefaultPostParseHandler(root, subDir),
                new MyRecursiveParserWrapper(new AutoDetectParser(),
                        new BasicContentHandlerFactory(BasicContentHandlerFactory.HANDLER_TYPE.TEXT, -1)));

        Detector detector = TikaConfig.getDefaultConfig().getDetector();
        MediaType mediaType = null;
        Metadata metadata = new Metadata();
        metadata.set(Metadata.RESOURCE_NAME_KEY, inputFile.getFileName().toString());
        try (InputStream is = TikaInputStream.get(inputFile)) {
            mediaType = detector.detect(is, metadata);
        }
        ParseContext parseContext = new ParseContext();
        if (pstUnraveler.getSupportedTypes(parseContext).contains(mediaType)) {
            try (InputStream is = TikaInputStream.get(inputFile)) {
                pstUnraveler.parse(is, new DefaultHandler(), new Metadata(), parseContext);
            }
        } else {
            LOG.info("Skipping "+inputFile.toString() +", detected as non-supported file type: "+mediaType.toString());
        }

    }

    private static void USAGE() {
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp(
                80,
                "java -jar unravel.jar -i my_pst.pst -o <outdir>",
                "Unravel PSTs",
                OPTIONS,
                "");

    }
}
