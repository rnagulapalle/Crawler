/**
 * @author Julius Seporaitis <julius@seporaitis.net>
 */
package net.seporaitis.quotes.hadoop.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;

import java.util.ArrayList;
import java.util.UUID;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.rdf.model.Literal;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.DCTerms;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.XSD;

import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.joda.time.DateTime;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Entities;

import net.seporaitis.crawler.hadoop.input.DocumentWritable;
import net.seporaitis.crawler.protobuf.BundleProtos;
import net.seporaitis.quotes.hadoop.input.ModelWritable;


/**
 * Quote mapper.
 *
 * Extracts quotes from news articles.
 */
public class QuoteMapper extends Mapper<Text, DocumentWritable, Text, ModelWritable> {

    private SentenceDetectorME detector;

    /* Reference:
     * Unicode Punctuation, Initial Quote: http://www.fileformat.info/info/unicode/category/Pi/list.htm
     * Unicode Punctuation, Final Quote: http://www.fileformat.info/info/unicode/category/Pf/list.htm
     */
    static String quoteStartRex = "^[\u00AB\u2018\u201B\u201E\u201F]{1}.*";
    static String quoteEndRex = ".*[\u00BB\u2019\u201C\u201D\u203A]{1},\\s?[–\\-]{1}\\s?.*";

    public void map(Text key, DocumentWritable value,
                    QuoteMapper.Context context) throws IOException,
                                                        InterruptedException {
        BundleProtos.Document document = value.getDocument();

        /* Load DOM tree */
        byte[] content = document.getContent().toByteArray();
        ByteArrayInputStream stream = new ByteArrayInputStream(content);
        Document dom = Jsoup.parse(stream, "UTF-8", "http://" + document.getDomain());
        dom.outputSettings().escapeMode(Entities.EscapeMode.extended);
        dom.outputSettings().charset("UTF-8");

        if (dom.select("div.article-cntnt").isEmpty()) {
            /* if no article is found */
            context.getCounter("net.seporaitis.quotes.nlp.hadoop.mapreduce.QuoteMapper",
                               "articleNotFound")
                .increment(1);
            return;
        }

        String text = dom.select("div.article-cntnt p").text();
        String[] listOfSentences = detector.sentDetect(text);
        ArrayList listOfQuotes = new ArrayList();
        Model model = ModelFactory.createDefaultModel();

        String normalizedDT;
        try {
            String dateTime = dom.select(".article-nfo .article-date").first().text();
            normalizedDT = normalizeDateTime(dateTime);
        } catch (Exception e) {
            /* date was not found */
            context.getCounter("net.seporaitis.quotes.nlp.hadoop.mapreduce.QuoteMapper",
                               "dateNotFound")
                .increment(1);
            return;
        }

        String title;
        try {
            title = dom.head().select("title").text();
        } catch (Exception e) {
            context.getCounter("net.seporaitis.quotes.nlp.hadoop.mapreduce.QuoteMapper",
                               "titleNotFound")
                .increment(1);
            return;
        }

        int i;
        String preQuote = "";
        String quote = "";
        String postQuote = "";

        /* 'inQuote' state flow:
         *
         * 1. false -> true: new quote started; add sentence before as context.
         * 2. true -> true: continuation of quote; append new sentence to quote.
         * 3. true -> false: quote ended; append last part of quote & add next
         *     sentence as context.
         */
        boolean inQuote = false;

        /* Various quote formats found in 'listOfSentences' (one line == one sentence):
         *
         * 1.
         * "quote quote quote.
         " Continued quote quote quote.", - text.
         *
         * 2.
         * "quote quote quote.", - text.
         *
         * 3.
         * "quote quote quote.",
         * - text.
         *
         * 4.
         * "quote quote quote.
         * " - text.
         *
         * NOTE: update this list for future reference.
         */

        for (i = 0; i < listOfSentences.length; i++) {
            /* If sentence matches quote start pattern. Append current sentence
             * to quote and set 'preQuote' to sentence before (this will be the
             * context).
             */
            if (listOfSentences[i].matches(QuoteMapper.quoteStartRex)
                && inQuote == false) {
                if (i > 0) {
                    preQuote = listOfSentences[i - 1];
                }
                inQuote = true;
                quote += listOfSentences[i];
            }

            /* Check if the quote matches the "quote end" pattern. If it does
             * set 'postQuote' to next sentence.
             *
             * Put the quote in the list and set 'inQuote' to false.
             */
            if (quote.matches(QuoteMapper.quoteEndRex)) {
                if ((i + 1) < listOfSentences.length) {
                    postQuote = listOfSentences[i + 1];
                    i = i + 1; // skip one sentence
                }

                /* Concatenate context and quote */
                String citation = "";
                if (preQuote.length() > 0) {
                    citation += preQuote + " ";
                }
                citation += quote;
                if (postQuote.length() > 0) {
                    citation += postQuote;
                }

                /* Quote identifier should be deterministic in case that in the
                 * future we would like to update the dataset, without
                 * regenerating everything.
                 */
                // TODO(julius): implement deterministic quote identifier.
                String uuid = UUID.randomUUID().toString();

                /* Create RDF Resource */
                Resource publisherRes = model.createResource("http://" +
                                                             document.getDomain() + "/");

                Literal issued = model.createTypedLiteral(normalizedDT,
                                                          XSD.dateTime.getURI().toString());

                Resource originalRes = model.createResource(document.getUri() +
                                                            "#" +
                                                            uuid)
                    .addProperty(RDF.type, DCTerms.BibliographicResource)
                    .addProperty(DCTerms.bibliographicCitation, citation)
                    .addProperty(DCTerms.title, title)
                    .addProperty(DCTerms.publisher, publisherRes)
                    .addLiteral(DCTerms.issued, issued);

                model.createResource("http://localhost/quote/" + uuid)
                    .addProperty(RDF.type, model.createProperty("http://rdfs.org/sioc/ns#",
                                                                "Post"))
                    .addProperty(model.createProperty("http://xmlns.com/foaf/spec/",
                                                      "primaryTopic"),
                                 originalRes);

                /* start over */
                preQuote = "";
                quote = "";
                postQuote = "";
                inQuote = false;
                continue;
            }

            /* If we are 'inQuote' mode - append sentence to quote (it's a
             * continuation).
             */
            if (inQuote == true) {
                quote += " " + listOfSentences[i];
            }
        }

        context.write(key, new ModelWritable(model));
    }

    @Override
    protected void setup(Context context) throws IOException,
                                                 InterruptedException {
        /* Load sentence detector */
        FileInputStream fis = new FileInputStream("models/lt-sent-model");
        SentenceModel model = new SentenceModel(fis);
        detector = new SentenceDetectorME(model);
    }

    protected String normalizeDateTime(String dateTime) {
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

        try {
            DateTime dt = new DateTime(Integer.parseInt(segments[0]), Integer.parseInt(segments[1]), Integer.parseInt(segments[2]), Integer.parseInt(segments[3]), Integer.parseInt(segments[4]));
            normalized = dt.toString("yyyy-MM-dd'T'HH:mm:'00Z'Z");
        } catch(Exception e) {
            // could not normalize date
            throw new RuntimeException("Could not normalize dateTime: '" + dateTime + "'.");
        }

        return normalized;
    }

}
