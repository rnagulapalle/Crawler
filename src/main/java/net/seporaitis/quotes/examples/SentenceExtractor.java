/**
 * @author Julius Seporaitis <julius@seporaitis.net>
 */
package net.seporaitis.quotes.examples;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.PrintStream;

import com.google.protobuf.CodedInputStream;

import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Entities;

import net.seporaitis.crawler.protobuf.BundleProtos;


/**
 * Loads a single Bundle file, sentence model and tries to extract sentences.
 */
public class SentenceExtractor {

    public static void main(String[] args) throws FileNotFoundException,
                                                  IOException {
        /* Load the data. */
        FileInputStream fis = new FileInputStream("input/15min_lt.2");
        CodedInputStream cis = CodedInputStream.newInstance(fis);
        cis.setSizeLimit(128 << 20);
        BundleProtos.Bundle bundle = BundleProtos.Bundle.parseFrom(cis);

        /* Load the sentence model. */
        InputStream is = new FileInputStream("lt-sent.model");
        SentenceModel model = new SentenceModel(is);
        SentenceDetectorME detector = new SentenceDetectorME(model);

        FileOutputStream fos = new FileOutputStream("sentences.out");
        PrintStream writer = new PrintStream(fos, true, "UTF-8");

        for (BundleProtos.Document document : bundle.getDocumentList()) {
            /* Skip non-news and comment pages. */
            if (document.getUri().indexOf("http://www.15min.lt/naujiena/") != 0) {
                continue;
            }
            if (document.getUri().indexOf("?rc=") > 0) {
                continue;
            }

            /* Load document content. */
            byte[] content = document.getContent().toByteArray();
            ByteArrayInputStream stream = new ByteArrayInputStream(content);
            Document dom = Jsoup.parse(stream, "UTF-8", "http://" + document.getDomain());
            dom.outputSettings().escapeMode(Entities.EscapeMode.extended);
            dom.outputSettings().charset("UTF-8");

            /* If article content element exists */
            if (dom.select("div.article-cntnt").size() > 0) {
                /* Retrieve text and detect sentences */
                String text = dom.select("div.article-cntnt p").text();
                String[] listOfSentences = detector.sentDetect(text);

                for (String sentence : listOfSentences) {
                    writer.println(sentence);
                }
            }
        }
    }

}
