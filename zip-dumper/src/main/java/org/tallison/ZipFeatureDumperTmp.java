package org.tallison;

import javax.xml.namespace.QName;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.math3.util.FastMath;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.XmlRootExtractor;
import org.apache.tika.io.CloseShieldInputStream;
import org.apache.tika.io.FilenameUtils;
import org.apache.tika.io.IOUtils;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.mime.MediaTypeRegistry;
import org.apache.tika.mime.MimeTypes;
import org.apache.tika.parser.pkg.ZipContainerDetector;

public class ZipFeatureDumperTmp {
/*
    private static final MediaType ZIP = MediaType.application("zip");
    private final MimeTypes mimeTypes = TikaConfig.getDefaultConfig().getMimeRepository();
    private final MediaTypeRegistry mediaTypeRegistry = TikaConfig.getDefaultConfig().getMediaTypeRegistry();
    private final ZipContainerDetector zipContainerDetector = new ZipContainerDetector();
    //mime :: feature :: count
    Map<String, Map<String, MutableInt>> globalFeatures = new HashMap<>();
    //feature :: mime :: weight
    Map<String, Map<String, Double>> weights = new HashMap<>();
    Map<String, MutableInt> classCounts = new HashMap<>();
    Map<String, Double> classPriors = new HashMap<>();

*/
/*
    public static void main(String[] args) throws Exception {
        ZipFeatureDumperTmp ex = new ZipFeatureDumperTmp();

        ex.execute(
                Paths.get(ZipFeatureDumper.class.getResource("/test-documents").toURI()));
        //feature :: mime :: count
        Map<String, Map<String, MutableInt>> features = new HashMap<>();
        for (String mime : featureClasses.keySet()) {
            for (Map.Entry<String, MutableInt> featureCount : featureClasses.get(mime).entrySet()) {
                String feature = featureCount.getKey();
                MutableInt i = featureCount.getValue();
                Map<String, MutableInt> m = features.get(feature);
                if (m == null) {
                    m = new HashMap<>();
                    features.put(feature, m);
                }
                m.put(mime, i);
            }
        }

        int classSum = 0;
        for (MutableInt count : ex.classCounts.values()) {
            classSum += count.getValue();
        }

        for (Map.Entry<String, MutableInt> classCount :ex.classCounts.entrySet()) {
            double prior = (double)classCount.getValue().getValue()/(double)classSum;
            double logPrior = -1/FastMath.log(2, prior);
            System.out.println(classCount.getKey() + " "+classCount.getValue() + " -> " +prior + " => "+logPrior);
            ex.classPriors.put(classCount.getKey(), logPrior);
        }

        Map<String, MutableInt> totalFeatureCounts = new HashMap<>();
        for (String mime : ex.globalFeatures.keySet()) {
            Map<String, MutableInt> features = ex.globalFeatures.get(mime);
            for (Map.Entry<String, MutableInt> f : features.entrySet()) {
                MutableInt sum = totalFeatureCounts.get(f.getKey());
                if (sum == null) {
                    sum = new MutableInt(0);
                    totalFeatureCounts.put(f.getKey(), sum);
                }
                sum.add(f.getValue());
            }
        }

        for (String mime : ex.globalFeatures.keySet()) {
            Map<String, MutableInt> features = ex.globalFeatures.get(mime);
            int sumForThisMime = 0;
            for (MutableInt i : features.values()) {
                sumForThisMime += i.getValue();
            }
            for (Map.Entry<String, MutableInt> e : features.entrySet()) {
                String feature = e.getKey();
                if (totalFeatureCounts.get(feature) == null || totalFeatureCounts.get(feature).getValue() < 5) {
                    continue;
                }
                double pFeatureGivenTheMime = (double) e.getValue().getValue() / (double) sumForThisMime;
                double weight = -1/FastMath.log(2, pFeatureGivenTheMime);
                System.out.println(mime + " "+feature+" "+weight);
                Map<String, Double> mimeWeights = ex.weights.get(feature);
                if (mimeWeights == null) {
                    mimeWeights = new HashMap<>();
                    ex.weights.put(feature, mimeWeights);
                }
                mimeWeights.put(mime, weight);
            }
        }
        for (String feature : ex.weights.keySet()) {
            System.out.println(feature);
            for (Map.Entry<String, Double> e : ex.weights.get(feature).entrySet()) {

                System.out.println( "\t"+e.getKey() + "\t"+e.getValue() +
                        " (prior: "+ex.classPriors.get(e.getKey()));
            }
        }



    }

    private void execute(Path path) {
        processDirectory(path);
    }

    private void processDirectory(Path path) {
        for (File f : path.toFile().listFiles()) {
            if (f.isDirectory()) {
                processDirectory(f.toPath());
            } else {
                System.out.println(f.getName());
                processFile(f);
            }
        }

    }

    private void processFile(File f) {
        Metadata metadata = new Metadata();
        try {
            try (TikaInputStream tis = TikaInputStream.get(f)) {
                MediaType mt = mimeTypes.detect(tis, metadata);
                System.out.println(mt + " -> " + mediaTypeRegistry.isSpecializationOf(mt, ZIP));
                //System.out.println(mt);
                if (mt.toString().contains("/zip") || mt.toString().contains("ooxml")) {
                    MediaType subType = zipContainerDetector.detect(tis, metadata);
                    extractFeatures(subType, tis);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            //swallow
        }
    }

    private void extractFeatures(MediaType subType, TikaInputStream tis) {
        if (subType == null) {
            return;
        }
        //for a given mime, here are the features and counts
        Map<String, MutableInt>  features = globalFeatures.get(subType.toString());
        increment(subType.toString(), classCounts);
        if (features == null) {
            features = new HashMap<>();
            globalFeatures.put(subType.toString(), features);
        }
        try {
            ZipArchiveInputStream zis = new ZipArchiveInputStream(new FileInputStream(tis.getFile()));

            ZipArchiveEntry ze = zis.getNextZipEntry();
            while (ze != null) {
                increment("full_name:"+ze.getName(), features);
                if (ze.isDirectory()) {
                    increment("directory_name:"+ze.getName(), features);
                } else {
                    String fileName = FilenameUtils.getName(ze.getName());

                    increment("file_name:"+fileName, features);
                    String[] rootDirs = getRootDirs(ze.getName());
                    for (int i = 0; i < rootDirs.length; i++) {
                        if (!StringUtils.isBlank(rootDirs[i])) {
                            increment("root_dir:" + rootDirs[i], features);
                        }
                    }
                    System.out.println(subType.toString()+"\t"+FilenameUtils.getName(ze.getName()));
                    int index = fileName.lastIndexOf(".");
                    if (index > -1) {
                        increment("extension:"+fileName.substring(index), features);
                    }
                }
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                if (ze.getName().endsWith(".rels")) {
                    Set<String> rels = StreamingZipContainerDetector.parseOOXMLRels(new CloseShieldInputStream(zis));
                    for (String r : rels) {
                        increment("rel:"+r, features);
                    }
                } else if (ze.getName().contains("app.xml")) {
                    ByteArrayOutputStream bos2 = new ByteArrayOutputStream();
                    IOUtils.copy(zis, bos2);
                    System.out.println("APP: " + new String(bos2.toByteArray(), StandardCharsets.UTF_8));
                } else if (ze.getName().endsWith(".xml")) {
                    QName root = new XmlRootExtractor().extractRootElement(zis);
                    increment("ns+local:"+root.getNamespaceURI()+":"+root.getLocalPart(), features);
                    increment("ns:"+root.getNamespaceURI(), features);
                    increment("local:"+root.getLocalPart(), features);
                }
                ze = zis.getNextZipEntry();
            }

        } catch (Exception e) {
            e.printStackTrace();
            //swallow
        }
    }

    private void increment(String name, Map<String, MutableInt> features) {
        MutableInt i = features.get(name);
        if (i == null) {
            i = new MutableInt(0);
            features.put(name, i);
        }
        i.increment();
    }

    private static String[] getRootDirs(String path) {
        int index = path.indexOf("/");
        String[] roots = new String[2];
        if (index > -1) {
            roots[0] = path.substring(0, index);
            index = path.indexOf("/", index+1);
            if (index > -1) {
                roots[1] = path.substring(0, index);
            }
        }
        return roots;
    }
    */
}
