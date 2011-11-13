package com.seporaitis.search;

import java.util.StringTokenizer;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Entities.EscapeMode;

import com.google.protobuf.CodedInputStream;
import com.seporaitis.crawler.protobuf.BundleProtos;
import com.seporaitis.ngram.NgramBuilder;

public class Search {
    
    static final int N = 3;

    /**
     * @param args
     * @throws IOException
     * @throws FileNotFoundException
     */
    public static void main(String[] args) throws FileNotFoundException,
            IOException {

        CodedInputStream inputStream = CodedInputStream.newInstance(new FileInputStream("output/15min_lt.1"));
        inputStream.setSizeLimit(128 << 20);
        BundleProtos.Bundle bundle = BundleProtos.Bundle.parseFrom(inputStream);
        
        PrintStream writer = new PrintStream(new FileOutputStream("output.html"), true, "UTF-8");
        
        int ngramCount = 0, excludedCount = 0, parsedCount = 0, lostCount = 0;
        for(BundleProtos.Document document : bundle.getDocumentList()) {
            if(document.getUri().indexOf("http://www.15min.lt/naujiena/") != 0) {
                excludedCount++;
                System.out.println("X\t" + document.getUri());
                continue;
            }
            if(document.getUri().indexOf("?rc=") > 0) {
                excludedCount++;
                System.out.println("X\t" + document.getUri());
                continue;
            }
            ByteArrayInputStream stream = new ByteArrayInputStream(document.getContent().toByteArray());
            Document dom = Jsoup.parse(stream, "UTF-8", "http://" + document.getDomain());
            dom.outputSettings().escapeMode(EscapeMode.extended);
            dom.outputSettings().charset("UTF-8");
            if(dom.select("div.article-cntnt").size() > 0) {
                parsedCount++;
                StringTokenizer tokenizer = new StringTokenizer(dom.select("div.article-cntnt p").text(), " \t\n\r\f.,!?:;()[]{}-", true);
                NgramBuilder builder = new NgramBuilder(tokenizer, Search.N);
                
                while(builder.hasMoreNgrams()) {
                    ngramCount++;
                    writer.println(builder.nextNgram());
                }
                
                /*
                Deque<String> ngrams = new ArrayDeque<String>(Search.N);
                while(tokenizer.hasMoreTokens()) {
                    String currToken = tokenizer.nextToken();
                    if(currToken.matches("[\\s\\t\\n\\r\\f]+")) {
                        continue;
                    }
                    ngrams.addLast(currToken);
                    if(ngrams.size() < Search.N) {
                        continue;
                    }
                    if(ngrams.size() > Search.N) {
                        ngrams.removeFirst();
                    }
                    ngramCount++;
                    StringBuilder builder = new StringBuilder();
                    Iterator<String> it = ngrams.iterator();
                    while(it.hasNext()) {
                        builder.append(it.next() + " ");
                    }
                    writer.println(builder.toString().trim());
                }
                */
                
            } else {
                System.out.println("L\t" + document.getUri());
                lostCount++;
            }
        }
        writer.close();
        
        System.out.println("Parsed:\t" + parsedCount + "\n" + "Excluded:\t" + excludedCount + "\n" + "Lost:\t" + lostCount);
        System.out.println("ngram size:\t" + Search.N);
        System.out.println("# of ngrams:\t" + ngramCount);
    }

}
