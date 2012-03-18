package com.seporaitis.nlp;

import com.google.protobuf.CodedInputStream;
import com.seporaitis.crawler.protobuf.BundleProtos;
import java.io.*;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Entities;

/**
 * Loads a single Bundle file, sentence model and tries to extract sentences.
 *
 * @author Julius Šėporaitis <julius@seporaitis.net>
 */
public class SentenceExtractor {

    public static void main(String[] args) throws FileNotFoundException,
            IOException {

        /*
         * Load the data.
         */
        CodedInputStream dataInputStream = CodedInputStream.newInstance(new FileInputStream("input/15min_lt.2"));
        dataInputStream.setSizeLimit(128 << 20);
        BundleProtos.Bundle bundle = BundleProtos.Bundle.parseFrom(dataInputStream);

        /*
         * Load sentence model.
         */
        InputStream modelInputStream = new FileInputStream("lt-sent.model");
        SentenceModel model = new SentenceModel(modelInputStream);

        SentenceDetectorME sentenceDetector = new SentenceDetectorME(model);

        PrintStream writer = new PrintStream(new FileOutputStream("sentences.out"), true, "UTF-8");

        for(BundleProtos.Document document : bundle.getDocumentList()) {
            /*
             * Skip non news item pages and comment pages
             */
            if(document.getUri().indexOf("http://www.15min.lt/naujiena/") != 0) {
                continue;
            }
            if(document.getUri().indexOf("?rc=") > 0) {
                continue;
            }

            /*
             * Load document content.
             */
            ByteArrayInputStream stream = new ByteArrayInputStream(document.getContent().toByteArray());
            Document dom = Jsoup.parse(stream, "UTF-8", "http://" + document.getDomain());
            dom.outputSettings().escapeMode(Entities.EscapeMode.extended);
            dom.outputSettings().charset("UTF-8");
            if(dom.select("div.article-cntnt").size() > 0) {
                String[] listOfSentences = sentenceDetector.sentDetect(dom.select("div.article-cntnt p").text());

                for(String sentence : listOfSentences) {
                    writer.println(sentence);
                }
            }
        }

    }
}
