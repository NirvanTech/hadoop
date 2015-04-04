package org.nirvan.practice.hadoop.multifileop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class MultiFileOPReducer extends Reducer<Text, Text, Text, Text> {
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
         }
         
     }

	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		mos.close();
		super.cleanup(context);
	}
	
  }