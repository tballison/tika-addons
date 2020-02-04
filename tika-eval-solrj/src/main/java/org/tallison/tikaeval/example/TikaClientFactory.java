package org.tallison.tikaeval.example;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.cli.CommandLine;

public class TikaClientFactory {

    public static TikaClient getClient(CommandLine commandLine) {
        if (commandLine.hasOption("e") && commandLine.hasOption("i")) {
            throw new IllegalArgumentException("can't have an extract directory _and_ an input directory");
        }
        if (commandLine.hasOption("i") && ! commandLine.hasOption("t")) {
            throw new IllegalArgumentException("must specify a tika-server if specifying an input directory");
        }

        //do more error checking
        if (commandLine.hasOption("t")) {
            if (commandLine.hasOption("f")) {
                return new TikaServerClient(TikaServerClient.INPUT_METHOD.FILE,
                        commandLine.getOptionValues("t"));
            } else {
                return new TikaServerClient(TikaServerClient.INPUT_METHOD.INPUTSTREAM,
                        commandLine.getOptionValues("t"));
            }
        }
        if (commandLine.hasOption("e")) {
            return new TikaExtractClient();
        }
        throw new IllegalArgumentException("must specify a tika-server url (-t)" +
                " or an extracts directory (-e)");
    }

    public static Path getRootDir(CommandLine commandLine) {
        if (commandLine.hasOption("i")) {
            return Paths.get(commandLine.getOptionValue("i"));
        } else if (commandLine.hasOption("e")) {
            return Paths.get(commandLine.getOptionValue("e"));
        }
        throw new IllegalArgumentException("must have either a raw doc input directory (-i) " +
                "or an extracts input directory (-e)");
    }
}
