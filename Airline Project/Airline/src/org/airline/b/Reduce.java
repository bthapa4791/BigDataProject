package org.airline.b;

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, Text, IntWritable, Text>{
	TreeMap<Integer, String> treeMap = new TreeMap<Integer, String>();
	protected void reduce(Text key, Iterable<Text> values, org.apache.hadoop.mapreduce.Reducer<Text,Text,IntWritable,Text>.Context context) throws java.io.IOException ,InterruptedException {
		for (Text text : values) {
			treeMap.put(Integer.parseInt(key.toString()), text.toString());
		}
	}
	protected void cleanup(org.apache.hadoop.mapreduce.Reducer<Text,Text,IntWritable,Text>.Context context) throws java.io.IOException ,InterruptedException {
		for(Entry<Integer, String> entry : treeMap.entrySet()) {
			  Integer key = entry.getKey();
			  String value = entry.getValue();
			  context.write(new IntWritable(key), new Text(value));
			}
	}
}
