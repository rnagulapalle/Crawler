package com.seporaitis.hadoop.mapreduce.lib.input;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.seporaitis.hadoop.helpers.DocumentWritable;

public class BundleInputFormat extends FileInputFormat<Text, DocumentWritable> {

    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }
    
    @Override
    public RecordReader<Text, DocumentWritable> createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException {
        
        return new BundleRecordReader();
    }
    
}
