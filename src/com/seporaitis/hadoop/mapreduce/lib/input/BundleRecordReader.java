package com.seporaitis.hadoop.mapreduce.lib.input;

import com.google.protobuf.CodedInputStream;
import com.seporaitis.crawler.protobuf.BundleProtos;
import com.seporaitis.hadoop.helpers.DocumentWritable;
import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class BundleRecordReader extends RecordReader<Text, DocumentWritable> {

    private Text key = null;
    private DocumentWritable value = null;
    
    private BundleProtos.Bundle bundle = null;
    private String fileName = null;
    
    private int pos = 0;
    private int size = 0;
    
    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {
        FileSplit split = (FileSplit)genericSplit;
        final Path file = split.getPath();
        
        fileName = file.getName();
        
        FileSystem fs = file.getFileSystem(context.getConfiguration());
        CodedInputStream inputStream = CodedInputStream.newInstance(fs.open(file));
        inputStream.setSizeLimit(128 << 20);
        
        bundle = BundleProtos.Bundle.parseFrom(inputStream);
        
        size = bundle.getDocumentCount();
    }
    
    @Override
    public void close() throws IOException {
        bundle = null;
    }

    @Override
    public Text getCurrentKey() throws IOException {
        return key;
    }

    @Override
    public DocumentWritable getCurrentValue() throws IOException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if(pos == size) {
            return 0.0f;
        }
        
        return (pos / size);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(key == null) {
            key = new Text();
        }
        
        if(value == null) {
            value = new DocumentWritable();
        }
        
        if(pos >= size) {
            return false;
        }
        
        key.set(fileName);
        value.set(bundle.getDocument(pos));
        
        pos++;
        
        return true;
        
    }

}
