package com.seporaitis.hadoop.helpers;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import com.google.protobuf.CodedInputStream;
import com.seporaitis.crawler.protobuf.BundleProtos;
import com.seporaitis.crawler.protobuf.BundleProtos.Document;

public class DocumentWritable implements WritableComparable<BundleProtos.Document> {
    
    private BundleProtos.Document document;
    
    public void set(BundleProtos.Document document) {
        this.document = document;
    }
    
    public BundleProtos.Document get() {
        return document;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        CodedInputStream cis = CodedInputStream.newInstance((DataInputStream)input);
        document = BundleProtos.Document.parseFrom(cis);
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.write(document.toByteArray());
    }

    @Override
    public int compareTo(Document otherDocument) {
        return document.getUri().compareTo(otherDocument.getUri());
    }

}
