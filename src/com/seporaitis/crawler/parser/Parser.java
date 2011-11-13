package com.seporaitis.crawler.parser;

import java.io.IOException;

import org.jsoup.nodes.Document;

import com.seporaitis.crawler.protobuf.BundleProtos;

public interface Parser {

    /**
     * Checks if the document is parsable by the Parser implementation.
     * 
     * @param document
     * @return
     */
    public boolean isParsable(Document dom, BundleProtos.Document document);
    
    /**
     * Parses the document and updates the T reference ref.
     * 
     * @param document
     * @param ref
     * @throws InterruptedException 
     * @throws IOException 
     */
    public void parse(Document dom, BundleProtos.Document document) throws IOException, InterruptedException;
}
