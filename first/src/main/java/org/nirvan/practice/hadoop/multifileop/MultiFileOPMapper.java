package org.nirvan.practice.hadoop.multifileop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public  class MultiFileOPMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text word = new Text();
    private Text word2 = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
       StringTokenizer tokenizer = new StringTokenizer(line,":");
           word.set(tokenizer.nextToken());
           word2.set(tokenizer.nextToken());
           context.write(word, word2);
   }
} 