/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.seporaitis.hadoop.mapreduce.nlp;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFWriter;
import com.seporaitis.hadoop.helpers.ModelWritable;
import java.io.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Quote mapper.
 * 
 * Extracts quotes in news articles.
 *
 * @author Julius Šėporaitis <julius@seporaitis.net>
 */
public class QuoteReducer extends Reducer<Text, ModelWritable, Text, Text> {
    
    private Text result = new Text();
    
    @Override
    public void reduce(Text key, Iterable<ModelWritable> listOfValues, QuoteReducer.Context context) throws IOException,
    InterruptedException {
        
        Model model = ModelFactory.createDefaultModel();
        
        for(ModelWritable value : listOfValues) {
            model = model.union(value.getModel());
        }
        
        RDFWriter writer = model.getWriter("TURTLE");
        writer.setProperty("tab", "4");
        writer.setProperty("relativeURIs", "same-document,relative");
        writer.setProperty("useTripleQuotedStrings", "true");
        
        StringWriter output = new StringWriter();
        writer.write(model, output, "http://local.quote.lt/");
        
        result.set(output.toString());
        context.write(null, result);
    }
    
}
