package com.seporaitis.nlp;

import com.google.protobuf.CodedInputStream;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.vocabulary.DCTerms;
import com.hp.hpl.jena.vocabulary.RDF;
import com.seporaitis.crawler.protobuf.BundleProtos;
import java.io.*;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.UUID;
import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import org.graphity.vocabulary.SIOC;
import org.joda.time.DateTime;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Entities;

/**
 * Loads a single bundle file, sentence model, extracts sentences and
 * then streams through them like this:
 *
 * 1. If sentence[i] starts with "„" - start quote and add
 * sentence[i - 1] as context.
 * 2. Increment i.
 * 3. If sentence[i] has "“, – " pattern, add sentence to quote. Add
 * sentence[i + 1] as context.
 * 4. Increment i and go to 1.
 */
public class QuoteExtractor {
    
    /** Reference:
     * Unicode Punctuation, Initial Quote: http://www.fileformat.info/info/unicode/category/Pi/list.htm
     * Unicode Punctuation, Final Quote: http://www.fileformat.info/info/unicode/category/Pf/list.htm
     */
    static String quoteStartRegEx = "^[\u00AB\u2018\u201B\u201E\u201F]{1}.*";
    static String quoteEndRegEx = ".*[\u00BB\u2019\u201C\u201D\u203A]{1},\\s?[–\\-]{1}\\s?.*"; //"“,\\s?–\\s?";
            
    public static void main(String[] args) throws FileNotFoundException,
                                                  IOException, 
                                                  NoSuchAlgorithmException {
        
        /* Load the data. */
        CodedInputStream dataStream = CodedInputStream.newInstance(new FileInputStream("input/15min_lt.2"));
        dataStream.setSizeLimit(128 << 20);
        BundleProtos.Bundle bundle = BundleProtos.Bundle.parseFrom(dataStream);

        /* Load sentence model */
        InputStream modelStream = new FileInputStream("lt-sent.model");
        SentenceModel sentenceModel = new SentenceModel(modelStream);
        SentenceDetectorME detector = new SentenceDetectorME(sentenceModel);
        
        /* Load named entity finder model */
        InputStream nefStream = new FileInputStream("lt-nef.model");
        TokenNameFinderModel nefModel = new TokenNameFinderModel(nefStream);
        NameFinderME nameFinder = new NameFinderME(nefModel);

        PrintStream writer = new PrintStream(new FileOutputStream("quotes.out"), true, "UTF-8");
        PrintStream console = new PrintStream(System.out, true, "UTF-8");
        
        Model model = ModelFactory.createDefaultModel();

        for(BundleProtos.Document document : bundle.getDocumentList()) {
            /* Skip non news item pages and comment pages */
            if(document.getUri().indexOf("http://www.15min.lt/naujiena/") != 0) {
                continue;
            }
            if(document.getUri().indexOf("?rc=") > 0) {
                continue;
            }

            /* Load document content. */
            ByteArrayInputStream stream = new ByteArrayInputStream(document.getContent().toByteArray());
            Document dom = Jsoup.parse(stream, "UTF-8", "http://" + document.getDomain());
            dom.outputSettings().escapeMode(Entities.EscapeMode.extended);
            dom.outputSettings().charset("UTF-8");
            
            if(dom.select("div.article-cntnt").size() > 0) {
                String[] listOfSentences = detector.sentDetect(dom.select("div.article-cntnt p").text());
                ArrayList listOfQuotes = new ArrayList();
                
                String articleDateTime;
                try {
                    articleDateTime = dom.select(".article-nfo .article-date").first().text();
                } catch(Exception e) {
                    console.println(document.getUri());
                    continue;
                }
                String normalizedDateTime = normalizeDateTime(articleDateTime);
                
                String articleTitle = dom.head().select("title").text();

                //System.out.println("Found " + listOfSentences.length + " sentences");
                int i;
                boolean inQuote = false;
                Quote quote = new Quote();
                String contextStart = "";
                String contextEnd = "";
                for(i = 0; i < listOfSentences.length; i++) {
                    /* If sentence matches quote start pattern. Add current sentence
                     * and sentence before to quote. */
                    if(listOfSentences[i].matches(QuoteExtractor.quoteStartRegEx)) {
                        if((i - 1) >= 0 && inQuote == false) {
                            quote.preSentence = listOfSentences[i - 1] + " ";
                        }
                        inQuote = true;
                        quote.quote += listOfSentences[i] + " ";
                    }
                    
                    /* Check if the quote has the quote end pattern. If it does sentence
                     * after this sentence to the quote, put the quote to the list and
                     * start over.
                     */
                    if(quote.quote.matches(QuoteExtractor.quoteEndRegEx)) {
                        inQuote = false;
                        if((i + 1) < listOfSentences.length) {
                            quote.postSentence = listOfSentences[i + 1];
                        }
                        listOfQuotes.add(quote);
                        quote = new Quote();
                        i = i + 1; // skip one sentence as it is added to the context
                        continue;
                    }
                    
                    /* If nothing else, but we are "in quote" mode - add the sentence
                     * to the quote and continue.
                     */
                    if(inQuote == true) {
                        quote.quote += listOfSentences[i];
                    }
                }
                
                Iterator it = listOfQuotes.iterator();
                while(it.hasNext()) {
                    quote = (Quote)it.next();
                    
                    MessageDigest md = MessageDigest.getInstance("MD5");
                    md.update(document.getUri().getBytes());
                    String uriSuffix = new BigInteger(1, md.digest()).toString(16);
                    
                    //Span[] names = nameFinder.find(quote);
                    String citation = "";
                    if(quote.preSentence.length() > 0) {
                        citation += quote.preSentence + " ";
                    }
                    citation += quote.quote;
                    if(quote.postSentence.length() > 0) {
                        citation += " " + quote.postSentence;
                    }
                    
                    Resource originalResource = model.createResource(document.getUri())
                            .addProperty(RDF.type, DCTerms.BibliographicResource)
                            .addProperty(DCTerms.bibliographicCitation, citation)
                            .addProperty(DCTerms.title, articleTitle)
                            .addProperty(DCTerms.publisher, model.createResource("http://" + document.getDomain() + "/"))
                            .addProperty(DCTerms.issued, normalizedDateTime);
                    
                    Resource quoteResource = model.createResource("http://local.quote.lt/citata/" + UUID.randomUUID().toString())
                            .addProperty(RDF.type, SIOC.POST)
                            .addProperty(model.createProperty("http://xmlns.com/foaf/spec/", "primaryTopic"), originalResource);
                }
            }
        }
        
        model.write(writer);
    }
    
    public static String normalizeDateTime(String dateTime) {
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
        System.out.println(dateTime);
        String[] segments = dateTime.split(" ", 5);
        
        DateTime dt = new DateTime(Integer.parseInt(segments[0]), Integer.parseInt(segments[1]), Integer.parseInt(segments[2]), Integer.parseInt(segments[3]), Integer.parseInt(segments[4]));
        normalized = dt.toString("yyyy-MM-dd'T'HH:mm:'00Z'Z");
        
        return normalized;
    }
}
