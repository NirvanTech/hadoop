package org.nirvan.practice.hadoop.multifileop;

  import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
          
  public class MultiFileOPMapreduce {
          
   public static class Map extends Mapper<LongWritable, Text, Text, Text> {
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
         
  public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private MultipleOutputs<Text, Text> mos;
		
	     @Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			super.setup(context);
			mos = new MultipleOutputs<Text, Text>(context);
		}
 
     public void reduce(Text key, Iterable<Text> values, Context context) 
       throws IOException, InterruptedException {
         for (Text val : values) {
        	 mos.write(key.toString(),key, val);
//        	 context.write(key, val);
         }
         
     }
     @Override
 	protected void cleanup(Context context)
 			throws IOException, InterruptedException {
 		mos.close();
 		super.cleanup(context);
 	}
  }
  
         
  public static void main(String[] args) throws Exception {
     Configuration conf = new Configuration();
         
     Job job = new Job(conf, "MultipleFiles");
     
     job.setOutputKeyClass(Text.class);
     job.setOutputValueClass(Text.class);
         
//     job.setMapperClass(Map.class);
//     job.setReducerClass(Reduce.class);
     
     job.setMapperClass(MultiFileOPMapper.class);
     job.setReducerClass(MultiFileOPReducer.class);
     
         
     job.setInputFormatClass(TextInputFormat.class);
     
//     job.setOutputFormatClass(TextOutputFormat.class);
     MultipleOutputs.addNamedOutput(job, "Age", TextOutputFormat.class,Text.class, Text.class);
     MultipleOutputs.addNamedOutput(job, "name",TextOutputFormat.class,Text.class, Text.class);
     
     FileInputFormat.addInputPath(job, new Path(args[0]));
     FileOutputFormat.setOutputPath(job, new Path(args[1]));
         
     job.waitForCompletion(true);
  }
         
 }