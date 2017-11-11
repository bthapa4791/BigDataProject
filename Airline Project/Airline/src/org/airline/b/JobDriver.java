package org.airline.b;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf  = new Configuration();
		Job job = new Job(conf, "airline");
		job.setJarByClass(JobDriver.class);
		job.setMapperClass(RouteMapper.class);
		job.setReducerClass(Reduce.class);
		
		DistributedCache.addCacheFile(new Path(args[0]).toUri(), job.getConfiguration());
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));	
		FileInputFormat.addInputPath(job, new Path(args[1]));	
		//MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, AirlineMapper.class);
		//MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RouteMapper.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
