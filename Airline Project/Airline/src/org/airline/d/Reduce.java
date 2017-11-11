package org.airline.d;

import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
	TreeMap<Integer, String> treeMap = new TreeMap<Integer, String>();
	protected void reduce(Text key, java.lang.Iterable<IntWritable> values, org.apache.hadoop.mapreduce.Reducer<Text,IntWritable,Text,IntWritable>.Context context) throws java.io.IOException ,InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		
		treeMap.put(sum, key.toString());
		
		if (treeMap.size() > 1) {
			treeMap.remove(treeMap.firstKey());
		}
	}
	protected void cleanup(Reducer<Text,IntWritable,Text,IntWritable>.Context context) throws java.io.IOException ,InterruptedException {
		Set<Integer> keys = treeMap.keySet();
		String country; 
        for(Integer key: keys){
            country = treeMap.get(key); 
            context.write(new Text(country), new IntWritable(key));
        }
	}
}
