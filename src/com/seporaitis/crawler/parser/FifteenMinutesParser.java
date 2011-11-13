package com.seporaitis.crawler.parser;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import com.seporaitis.crawler.protobuf.BundleProtos;
import com.seporaitis.hadoop.helpers.DocumentWritable;
import com.seporaitis.ngram.NgramBuilder;

public class FifteenMinutesParser {

    enum WordPosition {
        BODY, TITLE, INTRO, COMMENT_BODY, YEAR, MONTH, DAY
    }

    public static class NewsBody extends NgramParser {
        
        public NewsBody(Mapper<Text, DocumentWritable, Text, MapWritable>.Context context) {
            super(context);
        }

        @Override
        public boolean isParsable(Document dom, BundleProtos.Document document) {
            return (dom.select("div.article-cntnt").size() > 0);
        }
        
        @Override
        protected int getKey() {
            return WordPosition.BODY.ordinal();
        }
        
        @Override
        public void parse(
                Document dom,
                BundleProtos.Document document)
                throws IOException, InterruptedException {
            NgramBuilder builder = new NgramBuilder(this.getTokenizer(dom.select("div.article-cntnt p").text()), ngramSize);

            while (builder.hasMoreNgrams()) {
                String ngram = builder.nextNgram();
                if (ngram != null) {
                    key.set(this.getKey());
                    data.put(key, one);
                    context.write(new Text(ngram), data);
                }
            }
        }

        @Override
        protected Iterator<Element> getElements(Document dom) {
            return null;
        }

    }

    public static class NewsTitle extends NgramParser {
        
        public NewsTitle(Mapper<Text, DocumentWritable, Text, MapWritable>.Context context) {
            super(context);
        }

        @Override
        public boolean isParsable(Document dom,
                BundleProtos.Document document) {
            return (dom.select("div.article-content span a").size() > 0);
        }
        
        @Override
        protected Iterator<Element> getElements(Document dom) {
            return dom.select("div.article-content span a").iterator();
        }
        
        @Override
        protected int getKey() { return WordPosition.TITLE.ordinal(); }
    }

}
