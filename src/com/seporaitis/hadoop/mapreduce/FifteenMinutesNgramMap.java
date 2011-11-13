package com.seporaitis.hadoop.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Entities.EscapeMode;

import com.seporaitis.crawler.parser.FifteenMinutesParser;
import com.seporaitis.crawler.parser.Parser;
import com.seporaitis.crawler.protobuf.BundleProtos;
import com.seporaitis.hadoop.helpers.DocumentWritable;

public class FifteenMinutesNgramMap extends Mapper<Text, DocumentWritable, Text, MapWritable> {
    
    private List<Parser> listOfParsers;
    
    protected void setup(Mapper<Text, DocumentWritable, Text, MapWritable>.Context context) throws IOException, 
    InterruptedException {
        super.setup(context);
        
        listOfParsers = new ArrayList<Parser>();
        listOfParsers.add(new FifteenMinutesParser.NewsBody(context));
        listOfParsers.add(new FifteenMinutesParser.NewsTitle(context));
    }
    
    public void map(Text key, DocumentWritable value, Mapper<Text, DocumentWritable, Text, MapWritable>.Context context) throws IOException,
    InterruptedException {
        BundleProtos.Document document = value.get();
        
        ByteArrayInputStream stream = new ByteArrayInputStream(document.getContent().toByteArray());
        Document dom = Jsoup.parse(stream, "UTF-8", "http://" + document.getDomain());
        dom.outputSettings().escapeMode(EscapeMode.extended);
        dom.outputSettings().charset("UTF-8");
        
        Iterator<Parser> it = listOfParsers.iterator();
        while(it.hasNext()) {
            Parser p = (Parser)it.next();
            if(p.isParsable(dom, document)) {
                p.parse(dom, document);
            }
        }
    }
}
