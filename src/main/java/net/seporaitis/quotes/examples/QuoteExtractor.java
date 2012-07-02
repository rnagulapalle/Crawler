/**
 * @author Julius Seporaitis <julius@seporaitis.net>
 */
package net.seporaitis.quotes.examples;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import java.io.IOException;
import java.io.PrintStream;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.UUID;

import com.google.protobuf.CodedInputStream;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.DCTerms;
import com.hp.hpl.jena.vocabulary.RDF;

import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Entities;

import org.joda.time.DateTime;

import net.seporaitis.crawler.protobuf.BundleProtos;


/**
 * Loads a single bundle file, sentence model, extracts sentences and
 * then streams through them like this:
 *
 * 1. If sentence[i] starts with "„" - start quote and add
 * sentence[i - 1] as context.
 * 2. Increment i.
 * 3. If sentence[i] has "“, – " pattern, add sentence to quote. Add
 * sentence[i + 1] as context.
 * 4. Increment i and go to step 1.
 */
public class QuoteExtractor {

    /** Reference:
     * Unicode Punctuation, Initial Quote: http://www.fileformat.info/info/unicode/category/Pi/list.htm
     * Unicode Punctuation, Final Quote: http://www.fileformat.info/info/unicode/category/Pf/list.htm
     */
    static String quoteStartRex = "^[\u00AB\u2018\u201B\u201E\u201F]{1}.*";
    static String quoteEndRex = ".*[\u00BB\u2019\u201C\u201D\u203A]{1}," +
        "\\s?[–\\-]{1}\\s?.*"; //"“,\\s?–\\s?";

    public static void main(String[] args) throws FileNotFoundException,
                                                  IOException,
                                                  NoSuchAlgorithmException {
        /* Load the data. */
        FileInputStream fis = new FileInputStream("input/15min_lt.2");
        CodedInputStream dataStream = CodedInputStream.newInstance(fis);
        dataStream.setSizeLimit(128 << 20);
        BundleProtos.Bundle bundle = BundleProtos.Bundle.parseFrom(dataStream);

        /* Load sentence model */
        InputStream modelStream = new FileInputStream("lt-sent.model");
        SentenceModel sentenceModel = new SentenceModel(modelStream);
        SentenceDetectorME detector = new SentenceDetectorME(sentenceModel);

        FileOutputStream fos = new FileOutputStream("quotes.out");
        PrintStream writer = new PrintStream(fos, true, "UTF-8");

        Model model = ModelFactory.createDefaultModel();

        for (BundleProtos.Document document : bundle.getDocumentList()) {
            /* Skip non news item pages and comment pages */
            String uri = document.getUri();
            if (uri.indexOf("http://www.15min.lt/naujiena/") != 0) {
                continue;
            }
            if (uri.indexOf("?rc=") > 0) {
                continue;
            }

            /* Load document content. */
            byte[] content = document.getContent().toByteArray();
            ByteArrayInputStream stream = new ByteArrayInputStream(content);
            Document dom = Jsoup.parse(stream, "UTF-8", "http://" + document.getDomain());
            dom.outputSettings().escapeMode(Entities.EscapeMode.extended);
            dom.outputSettings().charset("UTF-8");

            if (dom.select("div.article-cntnt").size() > 0) {
                String text = dom.select("div.article-cntnt p").text();
                String[] listOfSentences = detector.sentDetect(text);
                ArrayList listOfQuotes = new ArrayList();

                String articleDT;
                try {
                    articleDT = dom.select(".article-nfo .article-date").first().text();
                } catch (Exception e) {
                    System.out.println(uri);
                    continue;
                }

                String normalizedDT = normalizeDateTime(articleDT);
                String title = dom.head().select("title").text();

                boolean inQuote = false;
                Quote quote = new Quote();
                int i = 0;
                for (i = 0; i < listOfSentences.length; i++) {
                    /* If sentence matches quote start pattern. Add
                     * current sentence before to quote.
                     */
                    if (listOfSentences[i].matches(QuoteExtractor.quoteStartRex)) {
                        if ((i - 1) >= 0 && inQuote == false) {
                            quote.preContext = listOfSentences[i - 1] + " ";
                        }
                        inQuote = true;
                        quote.text += listOfSentences[i] + " ";
                    }

                    /* Check if the quote has the quote end pattern. If it does,
                     * sentence after this sentence is postContext context.
                     */
                    if (quote.text.matches(QuoteExtractor.quoteEndRex)) {
                        inQuote = false;
                        if ((i + 1) < listOfSentences.length) {
                            quote.postContext = listOfSentences[i + 1];
                        }
                        listOfQuotes.add(quote);
                        quote = new Quote();
                        i = i + 1;
                        continue;
                    }

                    /* If nothing else, but we are "inQuote" mode - add the
                     * sentence to the quote and continue.
                     */
                    if (inQuote == true) {
                        quote.text += listOfSentences[i];
                    }
                }

                Iterator it = listOfQuotes.iterator();
                while (it.hasNext()) {
                    quote = (Quote)it.next();

                    MessageDigest md = MessageDigest.getInstance("MD5");
                    md.update(uri.getBytes());
                    String uriSuffix = new BigInteger(1, md.digest()).toString(16);

                    String citation = "";
                    if (quote.preContext.length() > 0) {
                        citation += quote.preContext + " ";
                    }
                    citation += quote.text;
                    if (quote.postContext.length() > 0) {
                        citation += " " + quote.postContext;
                    }

                    Resource baseUri = model.createResource("http://" + document.getDomain() + "/");
                    Resource originalResource = model.createResource(uri)
                        .addProperty(RDF.type, DCTerms.BibliographicResource)
                        .addProperty(DCTerms.bibliographicCitation, citation)
                        .addProperty(DCTerms.title, title)
                        .addProperty(DCTerms.publisher, baseUri)
                        .addProperty(DCTerms.issued, normalizedDT);

                    Resource quoteResource = model.createResource("http://localhost/quote/" +
                                                                  UUID.randomUUID().toString())
                        .addProperty(RDF.type, model.createProperty("http://rdfs.org/sioc/ns#",
                                                                    "Post"))
                        .addProperty(model.createProperty("http://xmlns.com/foaf/spec/",
                                                          "primaryTopic"),
                                     originalResource);
                }
            }
        }

        model.write(writer);
    }

    protected static String normalizeDateTime(String dateTime) {
        String normalized;

        dateTime = dateTime.replaceFirst("sausio", "01");
        dateTime = dateTime.replaceFirst("vasario", "02");
        dateTime = dateTime.replaceFirst("kovo", "03");
        dateTime = dateTime.replaceFirst("balandžio", "04");
        dateTime = dateTime.replaceFirst("gegužės", "05");
        dateTime = dateTime.replaceFirst("birželio", "06");
        dateTime = dateTime.replaceFirst("liepos", "07");
        dateTime = dateTime.replaceFirst("rugpjūčio", "08");
        dateTime = dateTime.replaceFirst("rugsėjo", "09");
        dateTime = dateTime.replaceFirst("spalio", "10");
        dateTime = dateTime.replaceFirst("lapkričio", "11");
        dateTime = dateTime.replaceFirst("gruodžio", "12");

        dateTime = dateTime.replaceFirst("d\\.", "");
        dateTime = dateTime.replaceFirst("\\.", " ");
        dateTime = dateTime.replaceFirst(":", " ");
        String[] segments = dateTime.split(" ", 5);

        DateTime dt = new DateTime(Integer.parseInt(segments[0]),
                                   Integer.parseInt(segments[1]),
                                   Integer.parseInt(segments[2]),
                                   Integer.parseInt(segments[3]),
                                   Integer.parseInt(segments[4]));
        normalized = dt.toString("yyyy-MM-dd'T'HH:mm:'00Z'Z");

        return normalized;
    }
}
