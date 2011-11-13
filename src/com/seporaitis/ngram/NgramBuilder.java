package com.seporaitis.ngram;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.StringTokenizer;

public class NgramBuilder implements Enumeration<String> {
    
    protected StringTokenizer tokenizer;
    protected int ngramSize = 2;
    protected Deque<String> listOfGrams;
    protected String skipList = "[\\s\\t\\n\\r\\f]+";
    
    public NgramBuilder(StringTokenizer tokenizer, int ngramSize) {
        this.tokenizer = tokenizer;
        this.ngramSize = ngramSize;
        this.listOfGrams = new ArrayDeque<String>(ngramSize);
    }
    
    public NgramBuilder(StringTokenizer tokenizer, int ngramSize, String skipList) {
        this.tokenizer = tokenizer;
        this.ngramSize = ngramSize;
        this.skipList = skipList;
        this.listOfGrams = new ArrayDeque<String>(ngramSize);
    }

    @Override
    public boolean hasMoreElements() {
        return hasMoreNgrams();
    }
    
    public boolean hasMoreNgrams() {
        return tokenizer.hasMoreTokens();
    }

    @Override
    public String nextElement() {
        return nextNgram();
    }
    
    public String nextNgram() {
        while(tokenizer.hasMoreTokens()) {
            String currToken = tokenizer.nextToken();
            if(currToken.matches(skipList)) {
                continue;
            }
            listOfGrams.addLast(currToken);
            if(listOfGrams.size() < ngramSize) {
                continue;
            }
            if(listOfGrams.size() > ngramSize) {
                listOfGrams.removeFirst();
            }
            StringBuilder builder = new StringBuilder();
            Iterator<String> it = listOfGrams.iterator();
            while(it.hasNext()) {
                builder.append(it.next() + " ");
            }
            
            return builder.toString().trim();
        }
        
        return null;
    }

}
