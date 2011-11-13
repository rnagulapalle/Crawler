package com.seporaitis.hadoop.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Entities.EscapeMode;

import com.seporaitis.crawler.protobuf.BundleProtos;
import com.seporaitis.hadoop.helpers.DocumentWritable;
import com.seporaitis.ngram.NgramBuilder;

public class ExampleNgramMap extends Mapper<Text, DocumentWritable, Text, LongWritable> {

    private final static LongWritable one = new LongWritable(1);
    private Text word = new Text();
    
    public void map(Text key, DocumentWritable value, Mapper<Text, DocumentWritable, Text, LongWritable>.Context context) throws IOException,
    InterruptedException {
        BundleProtos.Document document = value.get();
        
        ByteArrayInputStream stream = new ByteArrayInputStream(document.getContent().toByteArray());
        Document dom = Jsoup.parse(stream, "UTF-8", "http://" + document.getDomain());
        dom.outputSettings().escapeMode(EscapeMode.extended);
        dom.outputSettings().charset("UTF-8");
        
        if(dom.select("div.article-cntnt").size() > 0) {
            StringTokenizer tokenizer = new StringTokenizer(dom.select("div.article-cntnt p").text(), " \t\n\r\f.,!?:;()[]{}-“\"'”„", true);
            int ngramCount = context.getConfiguration().getInt("com.seporaitis.ngramCount", 1);
            NgramBuilder builder = new NgramBuilder(tokenizer, ngramCount);
            
            while(builder.hasMoreNgrams()) {
                String ngram = builder.nextNgram();
                if(ngram != null) {
                    word.set(ngram);
                    context.write(word, one);
                }
            }
        }
    }
}
