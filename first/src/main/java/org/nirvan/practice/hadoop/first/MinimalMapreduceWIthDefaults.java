package org.nirvan.practice.hadoop.first;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MinimalMapreduceWIthDefaults extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if(args.length !=2){
			System.err.printf("Usage %s [Generic options] <Input> <output> \n",getClass().getSimpleName());
			return -1;
		}
		 // Create a new JobConf
//	     JobConf jobConf = new JobConf(getConf(), MinimalMapreduceWIthDefaults.class);
	     
	     // Specify various job-specific parameters   
	     Job job = new Job(getConf());
	     job.setJobName("myjob");
		job.setJarByClass(getClass());
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MinimalMapreduceWIthDefaults(), args);
		System.exit(exitCode);
	}
}
