/*
 * @author Julius Šėporaitis <julius@seporaitis.net>
 */
package com.seporaitis.hadoop.mapreduce.nlp;

import com.seporaitis.hadoop.helpers.ModelWritable;
import com.seporaitis.hadoop.mapreduce.lib.input.BundleInputFormat;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * To launch locally, use:
 * 
 * java -cp dist/crawler.jar:dist/lib/* -Xmx2048m com.seporaitis.hadoop.mapreduce.nlp.QuoteExtractorJob
 *
 * @author Julius Šėporaitis <julius@seporaitis.net>
 */
public class QuoteExtractorJob {
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        GenericOptionsParser options = new GenericOptionsParser(conf, args);
        String[] otherArgs = options.getRemainingArgs();
        
        conf.set("mapred.input.pathFilter.glob", "15min_lt.*");
        
        Job job = new Job(conf, "QuoteExtractorJob");
        
        job.setJarByClass(QuoteExtractorJob.class);
        job.setMapOutputValueClass(ModelWritable.class);
        job.setMapperClass(QuoteMapper.class);
        job.setReducerClass(QuoteReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(BundleInputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("output"));
        
        System.exit(job.waitForCompletion(true) ? 1 : 0);
    }
    
}
