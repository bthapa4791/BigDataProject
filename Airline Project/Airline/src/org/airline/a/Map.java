package org.airline.a;

import org.airline.utility.AirportConstant;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, Text>{
	public void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,Text>.Context context) 
	throws java.io.IOException ,InterruptedException {
		String[] parts = value.toString().split("[,]");
		String country  = parts[AirportConstant.airportCountry];
		if (country.equalsIgnoreCase("India")) {
			String airportName = parts[AirportConstant.airportName];
			context.write(new Text(airportName), new Text(" "));
		}
	}
}
