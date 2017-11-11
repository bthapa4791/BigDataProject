package org.airline.e;

import org.airline.utility.AirlineConstant;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, Text>{
	protected void map(LongWritable key, Text value, Mapper<LongWritable,Text,Text,Text>.Context context) throws java.io.IOException ,InterruptedException {
		String[] parts = value.toString().split(",");
		String country = parts[AirlineConstant.airlineCountry];
		String active = parts[AirlineConstant.airlineActive];
		if (country.equalsIgnoreCase("United States") && active.equalsIgnoreCase("Y")) {
			context.write(new Text(parts[AirlineConstant.airlineName]), new Text(country.toString()));
		}
	}
}
