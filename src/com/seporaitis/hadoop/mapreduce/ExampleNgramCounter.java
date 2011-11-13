package com.seporaitis.hadoop.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.seporaitis.hadoop.mapreduce.lib.input.BundleInputFormat;

public class ExampleNgramCounter {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        GenericOptionsParser options = new GenericOptionsParser(conf, args);
        String[] otherArgs = options.getRemainingArgs();
        
        if(otherArgs.length == 0) {
            System.out.println("Missing argument: ngramCount.");
            System.exit(1);
        }
        
        conf.set("mapred.input.pathFilter.glob", "15min_lt.*");
        //conf.set("mapred.job.tracker", "local");
        //conf.set("fs.default.name", "file:///");
        
        conf.setInt("com.seporaitis.ngramCount", Integer.parseInt(otherArgs[0]));
        
        Job job = new Job(conf); 
        job.setJarByClass(ExampleNgramCounter.class);
        job.setMapperClass(ExampleNgramMap.class);
        job.setReducerClass(ExampleNgramReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setInputFormatClass(BundleInputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("output"));
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
