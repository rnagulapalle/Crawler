package com.seporaitis.crawler.parser;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import com.seporaitis.crawler.protobuf.BundleProtos;
import com.seporaitis.hadoop.helpers.DocumentWritable;
import com.seporaitis.ngram.NgramBuilder;

public abstract class NgramParser implements Parser {

    protected Mapper<Text, DocumentWritable, Text, MapWritable>.Context context;
    protected Text ngram = new Text();
    protected MapWritable data = new MapWritable();
    protected IntWritable key = new IntWritable();
    protected IntWritable one = new IntWritable(1);
    protected int ngramSize = 1;
    
    protected final static String splitCharacters = " \t\n\r\f.,!?:;()[]{}\\-“\"'”„";
    
    protected NgramParser() {
        throw new RuntimeException("Calling default constructor is prohibited.");
    }
    
    public NgramParser(Mapper<Text, DocumentWritable, Text, MapWritable>.Context context) {
        this.context = context;
        this.ngramSize = context.getConfiguration().getInt("com.seporaitis.ngramCount", 1);
    }
    
    @Override
    public void parse(Document dom,
            BundleProtos.Document document) throws IOException, InterruptedException {

        Iterator<Element> it = getElements(dom);
        while (it.hasNext()) {
            Element e = it.next();
            StringTokenizer tokenizer = getTokenizer(e.text());
            NgramBuilder builder = new NgramBuilder(tokenizer, ngramSize);

            while (builder.hasMoreNgrams()) {
                String value = builder.nextNgram();
                if (value != null) {
                    ngram.set(value);
                    key.set(getKey());
                    data.put(key, one);
                    context.write(ngram, data);
                }
            }
        }
    }
    
    /**
     * Returns string tokenizer
     * 
     * @return
     */
    protected StringTokenizer getTokenizer(String text) { 
        return new StringTokenizer(text, NgramParser.splitCharacters, true);
    }
    
    
    protected abstract Iterator<Element> getElements(Document dom);
    
    /**
     * Returns data key
     * 
     * @return
     */
    protected abstract int getKey();
}
