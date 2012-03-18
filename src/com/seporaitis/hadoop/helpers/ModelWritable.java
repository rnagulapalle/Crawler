/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.seporaitis.hadoop.helpers;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import java.io.*;
import org.apache.hadoop.io.WritableComparable;

/**
 * Jena Resource writable
 *
 * @author Julius Šėporaitis <julius@graphity.org>
 */
public class ModelWritable implements WritableComparable<ModelWritable> {
    
    private Model model;
    
    public ModelWritable() {
        this.model = ModelFactory.createDefaultModel();
    }
    
    public ModelWritable(Model model) {
        this.model = model;
    }
    
    public ModelWritable setModel(Model model) {
        this.model = model;
        return this;
    }
    
    public Model getModel() {
        return this.model;
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
        String str = input.readUTF();
        this.model = ModelFactory.createDefaultModel();
        this.model.read(new StringReader(str), "http://local.quote.lt/");
    }
    
    @Override
    public void write(DataOutput output) throws IOException {
        StringWriter writer = new StringWriter();
        this.model.write(writer);
        output.writeUTF(writer.toString());
    }
    
    @Override
    public int compareTo(ModelWritable other) {
        if(this.model.containsAll(other.getModel())) {
            return 0;
        } else if(this.model.size() > other.getModel().size()) {
            return 1;
        }
        
        return (-1);
    }
}
