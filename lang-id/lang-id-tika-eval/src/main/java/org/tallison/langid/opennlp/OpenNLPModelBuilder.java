package org.tallison.langid.opennlp;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;

import opennlp.tools.langdetect.DefaultLanguageDetectorContextGenerator;
import opennlp.tools.langdetect.LanguageDetectorContextGenerator;
import opennlp.tools.langdetect.LanguageDetectorFactory;
import opennlp.tools.langdetect.LanguageDetectorME;
import opennlp.tools.langdetect.LanguageDetectorModel;
import opennlp.tools.langdetect.LanguageDetectorSampleStream;
import opennlp.tools.langdetect.LanguageSample;
import opennlp.tools.ml.maxent.GISTrainer;
import opennlp.tools.ml.perceptron.PerceptronTrainer;
import opennlp.tools.util.InputStreamFactory;
import opennlp.tools.util.MarkableFileInputStreamFactory;
import opennlp.tools.util.ObjectStream;
import opennlp.tools.util.PlainTextByLineStream;
import opennlp.tools.util.TrainingParameters;
import opennlp.tools.util.model.ModelUtil;
import opennlp.tools.util.normalizer.CharSequenceNormalizer;
import opennlp.tools.util.normalizer.EmojiCharSequenceNormalizer;
import opennlp.tools.util.normalizer.NumberCharSequenceNormalizer;
import opennlp.tools.util.normalizer.ShrinkCharSequenceNormalizer;
import opennlp.tools.util.normalizer.TwitterCharSequenceNormalizer;

public class OpenNLPModelBuilder {

    private static CharSequenceNormalizer[] CHARSEQUENCE_NORMALIZERS = new CharSequenceNormalizer[]{
            TikaUrlCharSequenceNormalizer.getInstance(),
            AlphaIdeographSequenceNormalizer.getInstance(),
            EmojiCharSequenceNormalizer.getInstance(),
            TwitterCharSequenceNormalizer.getInstance(),
            NumberCharSequenceNormalizer.getInstance(),
            ShrinkCharSequenceNormalizer.getInstance()
    };

    public static void main(String[] args) throws IOException {
        Path trainingFile = Paths.get(args[0]);
        Path modelFile = Paths.get(args[1]);

        InputStreamFactory inputStreamFactory =
                new MarkableFileInputStreamFactory(trainingFile.toFile());

        ObjectStream<String> lineStream =
                new PlainTextByLineStream(inputStreamFactory, StandardCharsets.UTF_8);
        ObjectStream<LanguageSample> sampleStream = new LanguageDetectorSampleStream(lineStream);

        TrainingParameters mlParams = new TrainingParameters();
        //default
        mlParams.put(TrainingParameters.ALGORITHM_PARAM, GISTrainer.MAXENT_VALUE);
        mlParams.put(TrainingParameters.ITERATIONS_PARAM, 100);
        mlParams.put(TrainingParameters.CUTOFF_PARAM, 50);
        //
//        mlParams.put(TrainingParameters.ALGORITHM_PARAM,
  //              PerceptronTrainer.PERCEPTRON_VALUE);
    //    mlParams.put(TrainingParameters.CUTOFF_PARAM, 0);


        LanguageDetectorModel model = LanguageDetectorME.train(sampleStream, mlParams,
                new MyLanguageDetectorFactory());
        model.serialize(modelFile);
    }

    private static class MyLanguageDetectorFactory extends LanguageDetectorFactory {
        public MyLanguageDetectorFactory() {
            super();
        }

        @Override
        public LanguageDetectorContextGenerator getContextGenerator() {
            return new DefaultLanguageDetectorContextGenerator(1, 3, CHARSEQUENCE_NORMALIZERS);
        }
    }

    private static class TikaUrlCharSequenceNormalizer implements CharSequenceNormalizer {
        //use this custom copy/paste of opennlp to avoid long, long hang with mail_regex
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

    private static class AlphaIdeographSequenceNormalizer implements CharSequenceNormalizer {
        private static final Pattern REGEX = Pattern.compile("[^\\p{IsAlphabetic}\\p{IsIdeographic}]+");
        private static final AlphaIdeographSequenceNormalizer INSTANCE = new AlphaIdeographSequenceNormalizer();

        public static AlphaIdeographSequenceNormalizer getInstance() {
            return INSTANCE;
        }

        private AlphaIdeographSequenceNormalizer() {
        }

        @Override
        public CharSequence normalize(CharSequence charSequence) {
            return REGEX.matcher(charSequence).replaceAll(" ");
        }
    }
}
