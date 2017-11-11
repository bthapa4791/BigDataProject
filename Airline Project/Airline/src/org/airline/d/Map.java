package org.airline.d;

import org.airline.utility.AirportConstant;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
	protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,IntWritable>.Context context) throws java.io.IOException ,InterruptedException {
		String[] parts = value.toString().split(",");
		String country = parts[AirportConstant.airportCountry];
		context.write(new Text(country), new IntWritable(1));
	};
}
